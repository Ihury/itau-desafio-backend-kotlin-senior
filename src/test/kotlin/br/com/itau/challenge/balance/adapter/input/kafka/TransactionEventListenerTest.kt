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

    private lateinit var appender: ListAppender<ILoggingEvent>
    private val rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as Logger

    @BeforeEach
    fun captureLogs() {
        MDC.clear()
        appender = ListAppender<ILoggingEvent>().apply { start() }
        rootLogger.addAppender(appender)
        rootLogger.level = Level.DEBUG
    }

    @AfterEach
    fun releaseLogs() {
        rootLogger.detachAppender(appender)
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

        TransactionEventListener(parser, useCase).onMessage(record(validPayload.toByteArray()))

        assertEquals(1, useCase.events.size)
        assertEquals(parser.parse(validPayload.toByteArray()), useCase.events.single())
    }

    @Test
    fun `the mdc carries account, transaction and the topic partition offset correlation during the call and is cleared after`() {
        val useCase = RecordingUseCase()

        TransactionEventListener(parser, useCase).onMessage(record(validPayload.toByteArray(), topic = "t", partition = 3, offset = 99))

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

        val thrown = assertFailsWith<BalanceStoreUnavailableException> { TransactionEventListener(parser, useCase).onMessage(record(validPayload.toByteArray())) }

        assertSame(failure, thrown, "a excecao do caso de uso propaga intacta (nao e engolida)")
        assertEquals(mapOf("accountId" to null, "transactionId" to null, "correlationId" to null), allMdc())
    }

    @Test
    fun `a parser failure propagates without calling the use case and clears the mdc`() {
        val useCase = RecordingUseCase()
        val bad = validPayload.replace("\"BRL\"", "\"brl\"")

        val thrown = assertFailsWith<InvalidEventException> { TransactionEventListener(parser, useCase).onMessage(record(bad.toByteArray())) }

        assertEquals(RejectionReason.INVALID_CURRENCY, thrown.reason)
        assertTrue(useCase.events.isEmpty())
        assertEquals(mapOf("accountId" to null, "transactionId" to null, "correlationId" to null), allMdc())
    }

    @Test
    fun `a null value is a malformed payload`() {
        val useCase = RecordingUseCase()

        val thrown = assertFailsWith<InvalidEventException> { TransactionEventListener(parser, useCase).onMessage(record(null)) }

        assertEquals(RejectionReason.MALFORMED_PAYLOAD, thrown.reason)
        assertNull(thrown.detail)
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
        val listener = TransactionEventListener(parser, RecordingUseCase())

        runCatching { listener.onMessage(record(withSecret.toByteArray())) }
        runCatching { listener.onMessage(record("{\"x\":\"$secret\"".toByteArray())) }
        runCatching { listener.onMessage(record(validPayload.toByteArray())) }

        val logged = appender.list.joinToString("\n") { it.formattedMessage + it.throwableProxy?.message.orEmpty() }
        assertTrue(secret !in logged, "payload vazou para o log: $logged")
        assertTrue("183.12" !in logged && "315e3cfe" !in logged, "saldo/titular vazaram para o log")
    }
}
