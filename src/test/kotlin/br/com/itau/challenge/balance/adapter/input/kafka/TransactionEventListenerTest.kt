package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.domain.model.TransactionEvent
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.kafka.annotation.KafkaListener
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class TransactionEventListenerTest {
    private val parser = TransactionEventParser(Instant.parse("2000-01-01T00:00:00Z"), Instant.parse("1900-01-01T00:00:00Z"))

    private val validPayload =
        """{"transaction":{"id":"8e8ae808-b154-48b5-9f3e-553935cc4543","type":"CREDIT","amount":97.07,"currency":"BRL",""" +
            """"status":"APPROVED","timestamp":1751641364589998},"account":{"id":"5b19c8b6-0cc4-4c72-a989-0c2ee15fa975",""" +
            """"owner":"315e3cfe-f4af-4cd2-b298-a449e614349a","created_at":1634874339000000,"status":"ENABLED",""" +
            """"balance":{"amount":183.12,"currency":"BRL"}}}"""

    private class RecordingUseCase(
        private val behavior: (TransactionEvent) -> ApplyResult = { ApplyResult.Applied },
    ) : ProcessTransactionEventUseCase {
        val events = mutableListOf<TransactionEvent>()
        val mdcDuringCall = mutableListOf<Map<String, String?>>()

        override fun process(event: TransactionEvent): ApplyResult {
            events += event
            mdcDuringCall += listOf("accountId", "transactionId", "correlationId").associateWith { MDC.get(it) }
            return behavior(event)
        }
    }

    private val meters = SimpleMeterRegistry()

    private fun listener(useCase: ProcessTransactionEventUseCase) = TransactionEventListener(parser, useCase, meters)

    private fun ingestTimer(outcome: String) = meters.find("balance.ingest.duration").tag("outcome", outcome).timer()

    private lateinit var appender: ListAppender<ILoggingEvent>
    private val rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as Logger
    private var originalLevel: Level? = null

    @BeforeEach
    fun captureLogs() {
        MDC.clear()
        originalLevel = rootLogger.level
        appender = ListAppender<ILoggingEvent>().apply { start() }
        rootLogger.addAppender(appender)
        rootLogger.level = Level.DEBUG
    }

    @AfterEach
    fun releaseLogs() {
        rootLogger.detachAppender(appender)
        rootLogger.level = originalLevel
        MDC.clear()
    }

    private fun record(
        value: ByteArray?,
        topic: String = "transacoes-financeiras-processadas",
        partition: Int = 7,
        offset: Long = 1234,
    ) = ConsumerRecord<ByteArray?, ByteArray?>(topic, partition, offset, null, value)

    private fun allMdc(): Map<String, String?> = listOf("accountId", "transactionId", "correlationId").associateWith { MDC.get(it) }

    @Test
    fun `valid bytes are parsed and the use case is invoked once with the event`() {
        val useCase = RecordingUseCase()

        listener(useCase).onMessage(record(validPayload.toByteArray()))

        assertEquals(1, useCase.events.size)
        assertEquals(parser.parse(validPayload.toByteArray()), useCase.events.single())
    }

    @Test
    fun `the mdc carries account, transaction and the topic partition offset correlation during the call and is cleared after`() {
        val useCase = RecordingUseCase()

        listener(useCase).onMessage(record(validPayload.toByteArray(), topic = "t", partition = 3, offset = 99))

        assertEquals(
            mapOf(
                "accountId" to "5b19c8b6-0cc4-4c72-a989-0c2ee15fa975",
                "transactionId" to "8e8ae808-b154-48b5-9f3e-553935cc4543",
                "correlationId" to "t-3@99",
            ),
            useCase.mdcDuringCall.single(),
        )
        assertEquals(mapOf("accountId" to null, "transactionId" to null, "correlationId" to null), allMdc())
    }

    @Test
    fun `the mdc is cleared even when the use case throws`() {
        val failure = BalanceStoreUnavailableException(StoreFailureCause.TIMEOUT)
        val useCase = RecordingUseCase { throw failure }

        val thrown = assertFailsWith<BalanceStoreUnavailableException> { listener(useCase).onMessage(record(validPayload.toByteArray())) }

        assertSame(failure, thrown, "a excecao do caso de uso propaga intacta (nao e engolida)")
        assertEquals(mapOf("accountId" to null, "transactionId" to null, "correlationId" to null), allMdc())
    }

    @Test
    fun `a parser failure propagates without calling the use case and clears the mdc`() {
        val useCase = RecordingUseCase()
        val bad = validPayload.replace("\"BRL\"", "\"brl\"")

        val thrown = assertFailsWith<InvalidEventException> { listener(useCase).onMessage(record(bad.toByteArray())) }

        assertEquals(RejectionReason.INVALID_CURRENCY, thrown.reason)
        assertTrue(useCase.events.isEmpty())
        assertEquals(mapOf("accountId" to null, "transactionId" to null, "correlationId" to null), allMdc())
    }

    @Test
    fun `a null value is a malformed payload`() {
        val useCase = RecordingUseCase()

        val thrown = assertFailsWith<InvalidEventException> { listener(useCase).onMessage(record(null)) }

        assertEquals(RejectionReason.MALFORMED_PAYLOAD, thrown.reason)
        assertNull(thrown.fieldPath)
        assertTrue(useCase.events.isEmpty())
    }

    @Test
    fun `the listener signature has no acknowledgment because the commit belongs to the container`() {
        val method = TransactionEventListener::class.java.declaredMethods.single { it.isAnnotationPresent(KafkaListener::class.java) }

        assertEquals(listOf(ConsumerRecord::class.java), method.parameterTypes.toList())
        val annotation = method.getAnnotation(KafkaListener::class.java)
        assertEquals(listOf("\${balance.events.topic}"), annotation.topics.toList())
        assertEquals("transaction-event-listener", annotation.id)
        assertEquals(false, annotation.idIsGroup, "o grupo vem de spring.kafka.consumer.group-id")
    }

    @Test
    fun `nothing of the payload is logged`() {
        val secret = "SEGREDO-123"
        val withSecret = validPayload.replace("\"BRL\"", "\"$secret\"")
        val listener = listener(RecordingUseCase())

        runCatching { listener.onMessage(record(withSecret.toByteArray())) }
        runCatching { listener.onMessage(record("{\"x\":\"$secret\"".toByteArray())) }
        runCatching { listener.onMessage(record(validPayload.toByteArray())) }

        val logged = appender.list.joinToString("\n") { it.formattedMessage + it.throwableProxy?.message.orEmpty() }
        assertTrue(secret !in logged, "payload vazou para o log: $logged")
        assertTrue("183.12" !in logged && "315e3cfe" !in logged, "saldo/titular vazaram para o log")
    }

    @Test
    fun `the ingest duration is timed by the outcome of the use case`() {
        listOf(ApplyResult.Applied, ApplyResult.Obsolete, ApplyResult.Duplicate(conflicting = false), ApplyResult.Duplicate(conflicting = true), ApplyResult.Applied)
            .forEach { result -> listener(RecordingUseCase { result }).onMessage(record(validPayload.toByteArray())) }

        assertEquals(2L, ingestTimer("processed")?.count())
        assertEquals(1L, ingestTimer("obsolete")?.count())
        assertEquals(2L, ingestTimer("duplicate")?.count())
        assertEquals(0L, ingestTimer("rejected")?.count())
        assertEquals(0L, ingestTimer("error")?.count())
    }

    @Test
    fun `an invalid event is timed as rejected, from the parser or from the use case`() {
        val bad = validPayload.replace("\"BRL\"", "\"brl\"")
        assertFailsWith<InvalidEventException> { listener(RecordingUseCase()).onMessage(record(bad.toByteArray())) }
        assertFailsWith<InvalidEventException> { listener(RecordingUseCase()).onMessage(record(null)) }
        assertFailsWith<InvalidEventException> { listener(RecordingUseCase { throw InvalidEventException(RejectionReason.INVALID_TIMESTAMP) }).onMessage(record(validPayload.toByteArray())) }

        assertEquals(3L, ingestTimer("rejected")?.count())
        assertEquals(0L, ingestTimer("error")?.count())
    }

    @Test
    fun `any other failure is timed as error and still propagates`() {
        assertFailsWith<BalanceStoreUnavailableException> { listener(RecordingUseCase { throw BalanceStoreUnavailableException(StoreFailureCause.THROTTLED) }).onMessage(record(validPayload.toByteArray())) }
        assertFailsWith<IllegalStateException> { listener(RecordingUseCase { error("falha interna") }).onMessage(record(validPayload.toByteArray())) }

        assertEquals(2L, ingestTimer("error")?.count())
        assertEquals(0L, ingestTimer("processed")?.count())
    }

    @Test
    fun `the ingest timer has a histogram with the documented objectives and only the outcome tag`() {
        listener(RecordingUseCase()).onMessage(record(validPayload.toByteArray()))

        val timer = ingestTimer("processed")!!
        val buckets = timer.takeSnapshot().histogramCounts().map { it.bucket(java.util.concurrent.TimeUnit.MILLISECONDS) }
        assertTrue(5.0 in buckets && 2500.0 in buckets, "buckets $buckets")
        assertEquals(setOf("outcome"), meters.find("balance.ingest.duration").timers().flatMap { t -> t.id.tags.map { it.key } }.toSet())
    }
}
