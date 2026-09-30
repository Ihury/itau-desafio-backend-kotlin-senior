package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.BalanceStoreRejectedException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.domain.model.StoreFailureDetails
import br.com.itau.challenge.balance.testing.RecordingProcessingMetrics
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.BackOffHandler
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.listener.ListenerExecutionFailedException
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.kafka.support.SendResult
import software.amazon.awssdk.core.exception.SdkClientException
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Isolamento no DLT (US4) com o handler chamado diretamente e um `KafkaOperations` mockado, sem broker: destino, bytes
 * verbatim, headers, as tres classes de falha, a contagem de `rejected` e o comportamento com o DLT indisponivel.
 */
class DeadLetterConfigTest {
    private class RecordingBackOffHandler : BackOffHandler {
        val intervals = mutableListOf<Long>()

        override fun onNextBackOff(
            container: MessageListenerContainer?,
            exception: Exception?,
            nextBackOff: Long,
        ) {
            intervals += nextBackOff
        }

        override fun onNextBackOff(
            container: MessageListenerContainer,
            partition: TopicPartition,
            nextBackOff: Long,
        ) {
            intervals += nextBackOff
        }
    }

    private val dltTopic = "transacoes-financeiras-processadas.DLT"
    private val sent = CopyOnWriteArrayList<ProducerRecord<ByteArray, ByteArray>>()
    private var publishFailure: RuntimeException? = null

    @Suppress("UNCHECKED_CAST")
    private val template = mock(KafkaOperations::class.java) as KafkaOperations<ByteArray, ByteArray>
    private val metrics = RecordingProcessingMetrics()
    private val clock = Clock.fixed(Instant.parse("2026-06-01T12:00:00Z"), ZoneOffset.UTC)
    private val backOffs = RecordingBackOffHandler()
    private val config = DeadLetterConfig()

    /** Sem jitter, para as esperas serem exatas (o jitter e coberto por `BackpressureConfigTest`). */
    private val noJitter = BackOffProperties(initialMs = 500, maxMs = 30_000, jitterMs = 0)
    private val handler: DefaultErrorHandler = config.deadLetterErrorHandler(template, dltTopic, Duration.ofMillis(300), clock, metrics, noJitter, backOffs)
    private val consumer = mock(Consumer::class.java)
    private val container = mock(MessageListenerContainer::class.java)

    private val value = byteArrayOf(0xC3.toByte(), 0x28, 0x7B, 0x00, 0xFF.toByte())
    private val key = byteArrayOf(0x01, 0x02, 0x03)
    private val record =
        ConsumerRecord<Any, Any>("transacoes-financeiras-processadas", 7, 41L, key, value).also {
            it.headers().add(RecordHeader("origem", "autorizador".toByteArray()))
        }
    private val partition = TopicPartition(record.topic(), record.partition())

    init {
        doAnswer { invocation ->
            @Suppress("UNCHECKED_CAST")
            val outbound = invocation.getArgument<ProducerRecord<ByteArray, ByteArray>>(0)
            val failure = publishFailure
            if (failure != null) {
                CompletableFuture.failedFuture<SendResult<ByteArray, ByteArray>>(failure)
            } else {
                sent += outbound
                CompletableFuture.completedFuture(SendResult(outbound, RecordMetadata(TopicPartition(outbound.topic(), 1), 0L, 0, 0L, 0, 0)))
            }
        }.`when`(template).send(anyProducerRecord())
    }

    /** Matcher do Mockito que devolve um valor do tipo esperado (o `any()` devolve `null`, que o Kotlin recusa em tipo nao nulo). */
    private fun anyProducerRecord(): ProducerRecord<ByteArray, ByteArray> {
        any(ProducerRecord::class.java)
        return nullOf()
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> nullOf(): T = null as T

    private fun listenerFailure(cause: Exception) = ListenerExecutionFailedException("Listener failed", cause)

    /** Uma entrega que falhou; devolve `true` quando o registro foi recuperado (DLT) e `false` quando sera reentregue. */
    private fun deliver(cause: Exception): Boolean =
        try {
            handler.handleRemaining(listenerFailure(cause), listOf(record), consumer, container)
            true
        } catch (signal: RuntimeException) {
            // `RecordInRetryException` (package-private no Spring Kafka) e o sinal de que o registro sera reentregue
            if (signal.javaClass.simpleName != "RecordInRetryException") throw signal
            false
        }

    private fun header(
        outbound: ProducerRecord<ByteArray, ByteArray>,
        name: String,
    ): String? = outbound.headers().lastHeader(name)?.value()?.toString(Charsets.UTF_8)

    private fun onlyRecord(): ProducerRecord<ByteArray, ByteArray> {
        assertEquals(1, sent.size, "publicacoes no DLT")
        return sent.single()
    }

    // ----- permanente ----------------------------------------------------------------------------------------------

    @Test
    fun `an invalid event goes to the dlt on the first failure without redelivery`() {
        assertTrue(deliver(InvalidEventException(RejectionReason.INVALID_CURRENCY, "transaction.currency")))

        assertEquals(1, sent.size)
        assertEquals(emptyList(), backOffs.intervals, "sem espera nem reentrega")
        assertEquals(listOf("rejected(invalid_currency)"), metrics.outcomes)
    }

    @Test
    fun `the destination is the dlt topic with the partition left to the partitioner`() {
        deliver(InvalidEventException(RejectionReason.MISSING_FIELD, "account.id"))

        val outbound = onlyRecord()
        assertEquals(dltTopic, outbound.topic())
        assertNull(outbound.partition(), "partition -1 (o padrao 'mesma particao' falharia com 12 -> 3 particoes)")
    }

    @Test
    fun `key and value reach the dlt as the original bytes, binary included`() {
        deliver(InvalidEventException(RejectionReason.MALFORMED_PAYLOAD))

        val outbound = onlyRecord()
        assertContentEquals(value, outbound.value())
        assertContentEquals(key, outbound.key())
    }

    @Test
    fun `a record without key is published without key`() {
        val keyless = ConsumerRecord<Any, Any>("transacoes-financeiras-processadas", 3, 9L, null, value)

        handler.handleRemaining(listenerFailure(InvalidEventException(RejectionReason.MALFORMED_PAYLOAD)), listOf(keyless), consumer, container)

        assertNull(onlyRecord().key())
        assertContentEquals(value, onlyRecord().value())
    }

    @Test
    fun `the rejection headers and the original coordinates are present and no exception header leaks`() {
        deliver(InvalidEventException(RejectionReason.INVALID_TIMESTAMP, "account.created_at"))

        val outbound = onlyRecord()
        assertEquals("invalid_timestamp", header(outbound, "x-rejection-reason"))
        assertEquals("account.created_at", header(outbound, "x-rejection-detail"))
        assertEquals("2026-06-01T12:00:00Z", header(outbound, "x-rejected-at"))
        assertNotNull(outbound.headers().lastHeader("kafka_dlt-original-topic"))
        assertNotNull(outbound.headers().lastHeader("kafka_dlt-original-partition"))
        assertNotNull(outbound.headers().lastHeader("kafka_dlt-original-offset"))
        assertNotNull(outbound.headers().lastHeader("kafka_dlt-original-timestamp"))
        assertNotNull(outbound.headers().lastHeader("kafka_dlt-original-timestamp-type"))
        assertEquals("transacoes-financeiras-processadas", header(outbound, "kafka_dlt-original-topic"))
        val keys = outbound.headers().map { it.key() }
        assertTrue(keys.none { it.startsWith("kafka_dlt-exception") }, "headers de excecao vazam: $keys")
        assertTrue(keys.none { it.contains("stacktrace", ignoreCase = true) }, "pilha vaza: $keys")
    }

    @Test
    fun `headers never carry the exception text or values, only codes and paths`() {
        deliver(InvalidEventException(RejectionReason.INVALID_VALUE, "transaction.amount"))

        val everything = onlyRecord().headers().joinToString(" ") { "${it.key()}=${it.value().toString(Charsets.UTF_8)}" }
        assertFalse("Listener failed" in everything)
        assertFalse("ListenerExecutionFailedException" in everything)
        assertFalse("InvalidEventException" in everything)
    }

    @Test
    fun `the detail header is absent for a payload rejection without field path`() {
        deliver(InvalidEventException(RejectionReason.MALFORMED_PAYLOAD))

        assertNull(onlyRecord().headers().lastHeader("x-rejection-detail"))
    }

    // ----- nao classificada ----------------------------------------------------------------------------------------

    @Test
    fun `an unclassified failure gets three deliveries and then goes to the dlt as unprocessable event`() {
        assertFalse(deliver(IllegalStateException("defeito")), "1a entrega")
        assertEquals(0, sent.size)
        assertFalse(deliver(IllegalStateException("defeito")), "2a entrega")
        assertEquals(0, sent.size)
        assertTrue(deliver(IllegalStateException("defeito")), "3a entrega vai ao DLT")

        assertEquals(listOf(100L, 100L), backOffs.intervals)
        val outbound = onlyRecord()
        assertEquals("unprocessable_event", header(outbound, "x-rejection-reason"))
        assertNull(outbound.headers().lastHeader("x-rejection-detail"))
        assertEquals(listOf("rejected(unprocessable_event)"), metrics.outcomes)
        assertContentEquals(value, outbound.value())
    }

    @Test
    fun `a store rejection is unclassified and follows the same three deliveries`() {
        assertFalse(deliver(BalanceStoreRejectedException()))
        assertFalse(deliver(BalanceStoreRejectedException()))
        assertTrue(deliver(BalanceStoreRejectedException()))

        assertEquals("unprocessable_event", header(onlyRecord(), "x-rejection-reason"))
    }

    @Test
    fun `failures the spring default treats as fatal are also given three deliveries`() {
        assertFalse(deliver(ClassCastException("x")))
        assertFalse(deliver(ClassCastException("x")))
        assertTrue(deliver(ClassCastException("x")))

        assertEquals("unprocessable_event", header(onlyRecord(), "x-rejection-reason"))
    }

    // ----- transitoria ---------------------------------------------------------------------------------------------

    @Test
    fun `a transient failure never reaches the recoverer however many times it repeats`() {
        StoreFailureCause.entries.forEach { cause ->
            repeat(50) { assertFalse(deliver(BalanceStoreUnavailableException(cause)), "$cause na tentativa ${it + 1}") }
        }

        assertEquals(0, sent.size)
        verify(template, never()).send(anyProducerRecord())
        assertEquals(emptyList(), metrics.outcomes)
        assertEquals(0, metrics.dltPublishFailures)
        assertTrue(backOffs.intervals.all { it in 1..30_000 })
    }

    @Test
    fun `the wait of a transient failure grows exponentially up to thirty seconds and never runs out`() {
        repeat(20) { deliver(BalanceStoreUnavailableException(StoreFailureCause.UNAVAILABLE)) }

        assertEquals(listOf(500L, 1000L, 2000L, 4000L, 8000L, 16000L, 30000L, 30000L), backOffs.intervals.take(8))
        assertEquals(30_000L, backOffs.intervals.last())
    }

    @Test
    fun `a transient failure followed by a permanent one for the same record is isolated at once`() {
        repeat(3) { assertFalse(deliver(BalanceStoreUnavailableException(StoreFailureCause.THROTTLED))) }
        assertTrue(deliver(InvalidEventException(RejectionReason.INVALID_VALUE, "transaction.amount")))

        assertEquals("invalid_value", header(onlyRecord(), "x-rejection-reason"))
    }

    // ----- contagem e DLT indisponivel --------------------------------------------------------------------------------

    @Test
    fun `rejected is counted exactly once and only after the dlt confirms`() {
        publishFailure = RuntimeException("broker fora")
        assertFalse(deliver(InvalidEventException(RejectionReason.INVALID_IDENTIFIER, "account.id")))
        assertEquals(emptyList(), metrics.outcomes, "nada e contado enquanto o DLT nao confirma")

        publishFailure = null
        assertTrue(deliver(InvalidEventException(RejectionReason.INVALID_IDENTIFIER, "account.id")))

        assertEquals(listOf("rejected(invalid_identifier)"), metrics.outcomes)
    }

    @Test
    fun `a failing dlt publication leaves the record unconfirmed and redelivered, counts the failure and keeps the container alive`() {
        publishFailure = RuntimeException("broker fora")

        repeat(5) { assertFalse(deliver(InvalidEventException(RejectionReason.MISSING_FIELD, "account.id")), "tentativa ${it + 1}") }

        assertEquals(5, metrics.dltPublishFailures)
        assertEquals(emptyList(), metrics.outcomes)
        assertEquals(0, sent.size)
        verify(consumer, times(5)).seek(partition, record.offset())
        verify(consumer, never()).commitSync()
        verify(consumer, never()).commitAsync()
    }

    @Test
    fun `a dlt publication that never answers times out, is not confirmed and counts the failure`() {
        // o Spring espera max(delivery.timeout.ms + buffer, waitForSendResultTimeout): com o buffer zerado e o
        // delivery.timeout.ms curto do produtor, vale o waitForSendResultTimeout (300 ms neste teste)
        val producerFactory = mock(ProducerFactory::class.java)
        doReturn(mapOf<String, Any>("delivery.timeout.ms" to 100)).`when`(producerFactory).configurationProperties
        doReturn(producerFactory).`when`(template).producerFactory
        doAnswer { CompletableFuture<SendResult<ByteArray, ByteArray>>() }.`when`(template).send(anyProducerRecord())

        val started = System.nanoTime()
        assertFalse(deliver(InvalidEventException(RejectionReason.MALFORMED_PAYLOAD)))
        val elapsedMillis = Duration.ofNanos(System.nanoTime() - started).toMillis()

        assertTrue(elapsedMillis in 250..3000, "espera limitada pelo waitForSendResultTimeout (300 ms no teste): $elapsedMillis ms")
        assertEquals(1, metrics.dltPublishFailures)
        assertEquals(emptyList(), metrics.outcomes)
    }

    @Test
    fun `a dlt failure on the last delivery of an unclassified failure keeps the record and the next delivery isolates it`() {
        assertFalse(deliver(IllegalStateException("x")))
        assertFalse(deliver(IllegalStateException("x")))
        publishFailure = RuntimeException("broker fora")
        assertFalse(deliver(IllegalStateException("x")), "3a entrega: o DLT falhou, o registro e reentregue")
        assertEquals(1, metrics.dltPublishFailures)
        assertEquals(emptyList(), metrics.outcomes)

        publishFailure = null
        // o Spring zera a contagem de tentativas apos uma falha de recuperacao (`resetStateOnRecoveryFailure`): outras 3 entregas
        assertFalse(deliver(IllegalStateException("x")))
        assertFalse(deliver(IllegalStateException("x")))
        assertTrue(deliver(IllegalStateException("x")), "com o DLT de volta, o registro e isolado")

        assertEquals("unprocessable_event", header(onlyRecord(), "x-rejection-reason"))
        assertEquals(listOf("rejected(unprocessable_event)"), metrics.outcomes)
    }

    @Test
    fun `record level handling recovers the same way`() {
        assertTrue(handler.handleOne(listenerFailure(InvalidEventException(RejectionReason.INVALID_VALUE)), record, consumer, container))

        assertEquals("invalid_value", header(onlyRecord(), "x-rejection-reason"))
        assertEquals(listOf("rejected(invalid_value)"), metrics.outcomes)
    }

    @Test
    fun `the retry classification only makes the invalid event not retryable`() {
        // o `DefaultErrorHandler` padrao trataria ClassCastException como fatal (sem espera); aqui so o evento invalido e permanente
        assertFalse(deliver(ClassCastException("x")))
        assertEquals(listOf(100L), backOffs.intervals)
    }

    // ----- logs do ciclo de falha (FR-032, FR-035) -------------------------------------------------------------------

    private inline fun <T> capturingLogs(block: (ListAppender<ILoggingEvent>) -> T): T {
        val logger = LoggerFactory.getLogger(DeadLetterConfig::class.java) as Logger
        val appender = ListAppender<ILoggingEvent>().apply { start() }
        logger.addAppender(appender)
        try {
            return block(appender)
        } finally {
            logger.detachAppender(appender)
        }
    }

    @Test
    fun `an invalid event is logged once as an isolation with the reason and never as an unclassified failure`() {
        capturingLogs { logs ->
            deliver(InvalidEventException(RejectionReason.INVALID_CURRENCY, "transaction.currency"))

            assertTrue(logs.list.none { it.level == Level.ERROR }, "evento invalido e um desfecho esperado, nao um defeito: ${logs.list.map { it.formattedMessage }}")
            val isolated = logs.list.single { it.formattedMessage.startsWith("message isolated in the dlt") }
            assertEquals(Level.WARN, isolated.level)
            assertTrue("reason=invalid_currency" in isolated.formattedMessage && "detail=transaction.currency" in isolated.formattedMessage)
        }
    }

    private fun storeFailure(
        cause: StoreFailureCause,
        details: StoreFailureDetails?,
    ) = BalanceStoreUnavailableException(cause, SdkClientException.builder().message("segredo do sdk: 315e3cfe-f4af-4cd2-b298-a449e614349a").build(), details)

    @Test
    fun `a transient failure logs at warn the cause and the sdk diagnostics, never the free message, the payload or the stack`() {
        capturingLogs { logs ->
            deliver(storeFailure(StoreFailureCause.THROTTLED, StoreFailureDetails("software.amazon.awssdk.services.dynamodb.model.DynamoDbException", "ThrottlingException", 400)))

            val line = logs.list.single { it.formattedMessage.startsWith("store unavailable") }
            assertEquals(Level.WARN, line.level)
            assertTrue("cause=THROTTLED" in line.formattedMessage, line.formattedMessage)
            assertTrue("exception=software.amazon.awssdk.services.dynamodb.model.DynamoDbException" in line.formattedMessage, line.formattedMessage)
            assertTrue("errorCode=ThrottlingException" in line.formattedMessage && "statusCode=400" in line.formattedMessage, line.formattedMessage)
            assertFalse("segredo" in line.formattedMessage || "315e3cfe" in line.formattedMessage, "sem a mensagem livre do SDK")
            assertNull(line.throwableProxy, "sem pilha")
        }
    }

    @Test
    fun `a misconfigured store logs at error, still transient and never sent to the dlt`() {
        capturingLogs { logs ->
            val recovered = deliver(storeFailure(StoreFailureCause.MISCONFIGURED, StoreFailureDetails("software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException", "ResourceNotFoundException", 400)))

            assertFalse(recovered, "a falha de configuracao continua sendo retentada")
            assertTrue(sent.isEmpty(), "nunca vai ao DLT")
            val line = logs.list.single { it.formattedMessage.startsWith("store unavailable") }
            assertEquals(Level.ERROR, line.level)
            assertTrue("cause=MISCONFIGURED" in line.formattedMessage && "errorCode=ResourceNotFoundException" in line.formattedMessage, line.formattedMessage)
            assertNull(line.throwableProxy, "sem pilha")
        }
    }

    @Test
    fun `a transient failure without sdk diagnostics still logs the cause`() {
        capturingLogs { logs ->
            deliver(BalanceStoreUnavailableException(StoreFailureCause.UNAVAILABLE))

            val line = logs.list.single { it.formattedMessage.startsWith("store unavailable") }
            assertEquals(Level.WARN, line.level)
            assertTrue("cause=UNAVAILABLE" in line.formattedMessage, line.formattedMessage)
        }
    }

    @Test
    fun `the failure cycle logs carry the topic partition offset as correlation id and clean the mdc afterwards`() {
        capturingLogs { logs ->
            deliver(IllegalStateException("defeito"))
            deliver(BalanceStoreUnavailableException(StoreFailureCause.UNAVAILABLE))
            deliver(InvalidEventException(RejectionReason.MISSING_FIELD))

            val correlated = logs.list.filter { it.loggerName == DeadLetterConfig::class.java.name }
            assertTrue(correlated.size >= 3)
            correlated.forEach {
                assertEquals("transacoes-financeiras-processadas-7@41", it.mdcPropertyMap["correlationId"], it.formattedMessage)
            }
            assertNull(MDC.get("correlationId"), "o MDC do thread do consumer e sempre limpo")
        }
    }
}
