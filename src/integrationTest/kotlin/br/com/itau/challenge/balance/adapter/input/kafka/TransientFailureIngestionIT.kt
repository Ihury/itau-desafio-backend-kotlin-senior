package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbBalanceSnapshotWriter
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbClientProperties
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import br.com.itau.challenge.balance.support.EventPayloads
import br.com.itau.challenge.balance.support.IntegrationInfra
import br.com.itau.challenge.balance.support.KafkaITBase
import br.com.itau.challenge.balance.support.TopicSet
import br.com.itau.challenge.balance.support.TopicSet.Companion.SAME_PARTITION_KEY
import br.com.itau.challenge.balance.support.backpressureCount
import io.micrometer.core.instrument.MeterRegistry
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.core.exception.ApiCallTimeoutException
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.net.ConnectException
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.max
import kotlin.math.min
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TransientFailureIngestionIT : KafkaITBase() {
    override val topics: TopicSet
        get() = topicSet

    class ScriptedFailures {
        val pendingFailures = ConcurrentLinkedQueue<RuntimeException>()
        val accounts = ConcurrentHashMap.newKeySet<String>()
        val attemptsNanos = CopyOnWriteArrayList<Long>()
        val injected = AtomicInteger()
    }

    @TestConfiguration
    class Config {
        @Bean
        fun scriptedFailures(): ScriptedFailures = ScriptedFailures()

        @Bean
        @Primary
        fun failingWriter(
            @Qualifier("dynamoDbWriteClient") real: DynamoDbClient,
            properties: DynamoDbClientProperties,
            scriptedFailures: ScriptedFailures,
            meterRegistry: MeterRegistry,
        ): BalanceSnapshotWriter {
            val failing =
                Proxy.newProxyInstance(DynamoDbClient::class.java.classLoader, arrayOf(DynamoDbClient::class.java)) { _, method, args ->
                    val request = args?.firstOrNull() as? UpdateItemRequest
                    if (method.name == "updateItem" && request != null && request.key()["pk"]?.s()?.removePrefix("ACCOUNT#") in scriptedFailures.accounts) {
                        scriptedFailures.attemptsNanos += System.nanoTime()
                        scriptedFailures.pendingFailures.poll()?.let {
                            scriptedFailures.injected.incrementAndGet()
                            throw it
                        }
                    }
                    try {
                        method.invoke(real, *(args ?: emptyArray()))
                    } catch (failure: InvocationTargetException) {
                        throw failure.targetException
                    }
                } as DynamoDbClient
            val writer = DynamoDbBalanceSnapshotWriter(failing, properties.tableName, meterRegistry)
            return object : BalanceSnapshotWriter {
                override fun applyIfNewer(snapshot: BalanceSnapshot): ApplyResult = writer.applyIfNewer(snapshot)
            }
        }
    }

    private class PausedContainerSampler(
        private val anyContainerPaused: () -> Boolean,
    ) {
        private val sampling = AtomicBoolean(true)
        private val samples = AtomicInteger()
        val pausedSamples: Int get() = samples.get()

        private val thread =
            Thread {
                while (sampling.get()) {
                    if (anyContainerPaused()) samples.incrementAndGet()
                    Thread.sleep(SAMPLING_INTERVAL_MS)
                }
            }.also { it.isDaemon = true }

        fun start(): PausedContainerSampler = also { thread.start() }

        fun stop() {
            sampling.set(false)
            thread.join(STOP_TIMEOUT_MS)
        }

        private companion object {
            const val SAMPLING_INTERVAL_MS = 20L
            const val STOP_TIMEOUT_MS = 2_000L
        }
    }

    @Autowired
    private lateinit var scriptedFailures: ScriptedFailures

    private fun details(code: String) = AwsErrorDetails.builder().errorCode(code).errorMessage(code).serviceName("DynamoDB").build()

    private fun throughputExceeded(message: String): RuntimeException =
        ProvisionedThroughputExceededException
            .builder()
            .message(message)
            .statusCode(400)
            .awsErrorDetails(details("ProvisionedThroughputExceededException"))
            .build()

    private fun sdkFailuresThrottleConnectionTimeoutUnavailableThrottle(): List<RuntimeException> =
        listOf(
            throughputExceeded("The level of configured provisioned throughput for the table was exceeded"),
            SdkClientException.create("Unable to execute HTTP request: Connect to localhost:8000 failed", ConnectException("Connection refused")),
            ApiCallTimeoutException.create(2_000),
            DynamoDbException
                .builder()
                .message("Service Unavailable")
                .statusCode(503)
                .awsErrorDetails(details("ServiceUnavailable"))
                .build(),
            throughputExceeded("Throughput exceeded again"),
        )

    private fun backpressureByCause(): Map<String, Double> = EXPECTED_BACKPRESSURE_INCREMENT_BY_CAUSE.keys.associateWith { meterRegistry.backpressureCount(it) }

    private fun anyContainerPaused(): Boolean =
        (registry.getListenerContainer("transaction-event-listener") as ConcurrentMessageListenerContainer<*, *>).containers.any { it.isContainerPaused }

    private fun groupMembers(): Set<String> =
        IntegrationInfra.adminClient().use { admin ->
            admin
                .describeConsumerGroups(listOf(topics.groupId))
                .all()
                .get(BROKER_CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .getValue(topics.groupId)
                .members()
                .map { it.consumerId() }
                .toSet()
        }

    private fun backoffRangeMs(step: Int): LongRange {
        var base = BASE_BACKOFF_MS
        repeat(step) { base = min(base * 2, MAX_BACKOFF_MS) }
        val jitter = JITTER_STEP_MS * (base / BASE_BACKOFF_MS)
        return max(base - jitter, BASE_BACKOFF_MS)..min(base + jitter, MAX_BACKOFF_MS)
    }

    private fun assertBackpressureCountedWithTheRightCause(before: Map<String, Double>) {
        EXPECTED_BACKPRESSURE_INCREMENT_BY_CAUSE.forEach { (cause, increment) ->
            assertEquals(before.getValue(cause) + increment, meterRegistry.backpressureCount(cause), "backpressure{cause=$cause}")
        }
    }

    private fun assertBackoffGrowsWithinJitterRange(intervalsMs: List<Long>) {
        intervalsMs.forEachIndexed { step, waited ->
            val range = backoffRangeMs(step)
            assertTrue(waited >= range.first - LOWER_TOLERANCE_MS, "passo ${step + 1}: esperou $waited ms, abaixo de ${range.first} ms")
            assertTrue(waited <= range.last + SCHEDULING_SLACK_MS, "passo ${step + 1}: esperou $waited ms, acima de ${range.last} ms")
        }
        assertTrue(intervalsMs[4] > intervalsMs[0] && intervalsMs[3] > intervalsMs[0], "as esperas crescem: $intervalsMs")
    }

    @Test
    fun `sdk failures keep the valid message in the broker, grow the wait with the container paused, and it is processed afterwards`() {
        val dltBefore = topics.dlt.endOffsets()
        val membersBefore = groupMembers()
        assertEquals(CONSUMER_THREADS, membersBefore.size, "$CONSUMER_THREADS threads de consumo no grupo")
        val backpressureBefore = backpressureByCause()
        val account = newAccount()
        val neighbour = newAccount()
        scriptedFailures.accounts += account
        scriptedFailures.pendingFailures += sdkFailuresThrottleConnectionTimeoutUnavailableThrottle()
        val sampler = PausedContainerSampler(::anyContainerPaused).start()

        try {
            topics.publishInPartitionOf(SAME_PARTITION_KEY, EventPayloads.transaction(account, balanceAmount = "77.70"))
            topics.publishInPartitionOf(SAME_PARTITION_KEY, EventPayloads.transaction(neighbour, balanceAmount = "88.80"))

            await.atMost(Duration.ofSeconds(10)).until { scriptedFailures.injected.get() >= 2 }
            assertEquals(0, topics.dlt.countSince(dltBefore), "falha transitoria nunca leva mensagem valida ao DLT")
            assertTrue(topics.group.lag() > 0, "a mensagem continua no broker (nao confirmada)")
            assertEquals(404, get(account).statusCode(), "conta ainda nao gravada")

            await.atMost(Duration.ofSeconds(60)).until { get(account).statusCode() == 200 && get(neighbour).statusCode() == 200 }
        } finally {
            sampler.stop()
        }
        awaitBalance(account, "77.70")
        awaitBalance(neighbour, "88.80")
        topics.group.awaitLagZero()

        assertEquals(EXPECTED_INJECTED_FAILURES, scriptedFailures.injected.get(), "as $EXPECTED_INJECTED_FAILURES falhas injetadas foram entregues ao consumer")
        assertEquals(0, topics.dlt.countSince(dltBefore), "DLT com exatamente 0 mensagens")
        assertBackpressureCountedWithTheRightCause(backpressureBefore)

        val stamps = scriptedFailures.attemptsNanos.toList()
        assertEquals(EXPECTED_INJECTED_FAILURES + 1, stamps.size, "$EXPECTED_INJECTED_FAILURES falhas + 1 sucesso")
        val intervalsMs = stamps.zipWithNext { a, b -> (b - a) / NANOS_PER_MILLI }
        assertBackoffGrowsWithinJitterRange(intervalsMs)

        assertTrue(sampler.pausedSamples > 0, "o container deve ficar pausado durante o backoff")
        assertTrue(intervalsMs.max() > MAX_POLL_INTERVAL_MS, "alguma espera passou de max.poll.interval.ms ($MAX_POLL_INTERVAL_MS ms): $intervalsMs")
        assertEquals(membersBefore, groupMembers(), "nenhum consumer saiu do grupo (o poll continuou vivo durante a pausa)")
    }

    companion object {
        private const val CONSUMER_THREADS = 4
        private const val EXPECTED_INJECTED_FAILURES = 5
        private const val BASE_BACKOFF_MS = 500L
        private const val JITTER_STEP_MS = 250L
        private const val MAX_BACKOFF_MS = 30_000L
        private const val LOWER_TOLERANCE_MS = 100L
        private const val SCHEDULING_SLACK_MS = 2_000L
        private const val NANOS_PER_MILLI = 1_000_000L
        private const val MAX_POLL_INTERVAL_MS = 3_000L
        private const val BROKER_CALL_TIMEOUT_SECONDS = 15L
        private val EXPECTED_BACKPRESSURE_INCREMENT_BY_CAUSE = mapOf("throttled" to 2, "timeout" to 1, "unavailable" to 2)

        private val topicSet = TopicSet("it-transient")

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            topicSet.registerProperties(registry)
            // max.poll.interval.ms < backoff: so um container PAUSADO (nao dormindo) mantem o consumer vivo.
            registry.add("spring.kafka.consumer.properties.max.poll.interval.ms") { MAX_POLL_INTERVAL_MS.toString() }
            registry.add("spring.kafka.listener.poll-timeout") { "1s" }
        }
    }
}
