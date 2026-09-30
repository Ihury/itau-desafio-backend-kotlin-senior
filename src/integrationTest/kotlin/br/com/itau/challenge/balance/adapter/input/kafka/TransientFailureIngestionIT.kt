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

/**
 * Falhas transientes do armazenamento com as excecoes REAIS do SDK (`ProvisionedThroughputExceededException`,
 * `SdkClientException` de conexao, `ApiCallTimeoutException` e `DynamoDbException` 503), injetadas nas primeiras chamadas de
 * escrita de uma conta (o DynamoDB Local nao emula throttling). A mensagem valida NUNCA vai ao DLT, fica no broker (lag > 0) e
 * e processada quando a falha passa; o container fica PAUSADO durante a espera (o poll continua vivo: `max.poll.interval.ms`
 * de 3 s e esperas de ate 12 s, sem rebalance), a espera cresce e `balance.consumer.backpressure` conta cada falha com a
 * `cause` certa. Contexto e topicos proprios (`@TestConfiguration`).
 */
class TransientFailureIngestionIT : KafkaITBase() {
    override val topics: TopicSet
        get() = topicSet

    /** Falhas a lancar, em ordem, para a conta marcada; depois disso a escrita segue para o DynamoDB real. */
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

        /** Escritor real sobre um cliente que lanca as excecoes do SDK nas primeiras `updateItem` da conta marcada. */
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

    @Autowired
    private lateinit var scriptedFailures: ScriptedFailures

    private fun details(code: String) = AwsErrorDetails.builder().errorCode(code).errorMessage(code).serviceName("DynamoDB").build()

    /** As 5 falhas injetadas, em ordem: throttling, conexao, timeout da chamada, 503 e throttling. */
    private fun sdkFailures(): List<RuntimeException> =
        listOf(
            ProvisionedThroughputExceededException
                .builder()
                .message("The level of configured provisioned throughput for the table was exceeded")
                .statusCode(400)
                .awsErrorDetails(details("ProvisionedThroughputExceededException"))
                .build(),
            SdkClientException.create("Unable to execute HTTP request: Connect to localhost:8000 failed", ConnectException("Connection refused")),
            ApiCallTimeoutException.create(2_000),
            DynamoDbException
                .builder()
                .message("Service Unavailable")
                .statusCode(503)
                .awsErrorDetails(details("ServiceUnavailable"))
                .build(),
            ProvisionedThroughputExceededException
                .builder()
                .message("Throughput exceeded again")
                .statusCode(400)
                .awsErrorDetails(details("ProvisionedThroughputExceededException"))
                .build(),
        )

    private fun backpressure(cause: String): Double = meterRegistry.get("balance.consumer.backpressure").tag("cause", cause).counter().count()

    private fun listenerContainer(): ConcurrentMessageListenerContainer<*, *> = registry.getListenerContainer("transaction-event-listener") as ConcurrentMessageListenerContainer<*, *>

    /** Ids dos membros do grupo: se um consumer estoura `max.poll.interval.ms`, sai do grupo e reentra com outro id. */
    private fun groupMembers(): Set<String> =
        IntegrationInfra.adminClient().use { admin ->
            admin
                .describeConsumerGroups(listOf(topics.groupId))
                .all()
                .get(15, TimeUnit.SECONDS)
                .getValue(topics.groupId)
                .members()
                .map { it.consumerId() }
                .toSet()
        }

    @Test
    fun `sdk failures keep the valid message in the broker, grow the wait with the container paused, and it is processed afterwards`() {
        val dltBefore = topics.dltEndOffsets()
        val membersBefore = groupMembers()
        assertEquals(4, membersBefore.size, "4 threads de consumo no grupo")
        val before = mapOf("throttled" to backpressure("throttled"), "unavailable" to backpressure("unavailable"), "timeout" to backpressure("timeout"))
        val account = newAccount()
        val neighbour = newAccount()
        scriptedFailures.accounts += account
        scriptedFailures.pendingFailures += sdkFailures()

        // amostrador: o container fica pausado durante a espera do backoff (poll vivo, sem dormir no thread do poll)
        val sampling = AtomicBoolean(true)
        var pausedSamples = 0
        val sampler =
            Thread {
                while (sampling.get()) {
                    if (listenerContainer().containers.any { it.isContainerPaused }) pausedSamples++
                    Thread.sleep(20)
                }
            }.also {
                it.isDaemon = true
                it.start()
            }

        try {
            // mesma chave = mesma particao: a vizinha esta atras da mensagem que falha e tambem fica retida
            topics.publishKeyed("mesma-particao", EventPayloads.transaction(account, balanceAmount = "77.70"))
            topics.publishKeyed("mesma-particao", EventPayloads.transaction(neighbour, balanceAmount = "88.80"))

            await.atMost(Duration.ofSeconds(10)).until { scriptedFailures.injected.get() >= 2 }
            assertEquals(0, topics.dltCountSince(dltBefore), "falha transitoria nunca leva mensagem valida ao DLT")
            assertTrue(topics.groupLag() > 0, "a mensagem continua no broker (nao confirmada)")
            assertEquals(404, get(account).statusCode(), "conta ainda nao gravada")

            await.atMost(Duration.ofSeconds(60)).until { get(account).statusCode() == 200 && get(neighbour).statusCode() == 200 }
        } finally {
            sampling.set(false)
            sampler.join(2_000)
        }
        awaitBalance(account, "77.70")
        awaitBalance(neighbour, "88.80")
        topics.awaitGroupLagZero()

        assertEquals(5, scriptedFailures.injected.get(), "as 5 falhas injetadas foram entregues ao consumer")
        assertEquals(0, topics.dltCountSince(dltBefore), "DLT com exatamente 0 mensagens")

        // metrica: cada falha contada com a causa da classificacao (2 throttling, 1 timeout, 2 indisponibilidade)
        assertEquals(before.getValue("throttled") + 2, backpressure("throttled"))
        assertEquals(before.getValue("timeout") + 1, backpressure("timeout"))
        assertEquals(before.getValue("unavailable") + 2, backpressure("unavailable"))

        // as esperas crescem (500 ms x2, jitter de 250 ms escalado): 6 tentativas, 5 intervalos dentro da faixa de cada passo
        val stamps = scriptedFailures.attemptsNanos.toList()
        assertEquals(6, stamps.size, "5 falhas + 1 sucesso")
        val intervalsMs = stamps.zipWithNext { a, b -> (b - a) / 1_000_000 }
        println("BACKOFF-INTERVALS-MS=$intervalsMs")
        intervalsMs.forEachIndexed { step, waited ->
            var base = 500L
            repeat(step) { base = min(base * 2, 30_000L) }
            val jitter = 250L * (base / 500L)
            val low = max(base - jitter, 500L)
            val high = min(base + jitter, 30_000L)
            assertTrue(waited >= low - 100, "passo ${step + 1}: esperou $waited ms, abaixo de $low ms")
            assertTrue(waited <= high + 2_000, "passo ${step + 1}: esperou $waited ms, acima de $high ms")
        }
        assertTrue(intervalsMs[4] > intervalsMs[0] && intervalsMs[3] > intervalsMs[0], "as esperas crescem: $intervalsMs")

        // pausa: o container esteve pausado durante a espera e o poll seguiu vivo (esperas > max.poll.interval.ms de 3 s, sem rebalance)
        assertTrue(pausedSamples > 0, "o container deve ficar pausado durante o backoff")
        assertTrue(intervalsMs.max() > 3_000, "alguma espera passou de max.poll.interval.ms (3 s): $intervalsMs")
        assertEquals(membersBefore, groupMembers(), "nenhum consumer saiu do grupo (o poll continuou vivo durante a pausa)")
        println("PAUSED-SAMPLES=$pausedSamples")
    }

    companion object {
        private val topicSet = TopicSet("it-transient")

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            topicSet.registerProperties(registry)
            // Espera do backoff MAIOR que o intervalo de poll: so com o container pausado (e nao dormindo) o consumer sobrevive.
            registry.add("spring.kafka.consumer.properties.max.poll.interval.ms") { "3000" }
            registry.add("spring.kafka.listener.poll-timeout") { "1s" }
        }
    }
}
