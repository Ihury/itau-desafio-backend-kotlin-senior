package br.com.itau.challenge.balance.testing

import br.com.itau.challenge.balance.adapter.input.kafka.BackOffProperties
import br.com.itau.challenge.balance.adapter.input.kafka.DeadLetterConfig
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.mock
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.BackOffHandler
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.kafka.support.SendResult
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals

val NO_JITTER_BACK_OFF = BackOffProperties(initialMs = 500, maxMs = 30_000, jitterMs = 0)

val DEAD_LETTER_CLOCK: Clock = Clock.fixed(Instant.parse("2026-06-01T12:00:00Z"), ZoneOffset.UTC)

val ORIGINAL_VALUE = byteArrayOf(0xC3.toByte(), 0x28, 0x7B, 0x00, 0xFF.toByte())

val ORIGINAL_KEY = byteArrayOf(0x01, 0x02, 0x03)

const val SHORT_DELIVERY_TIMEOUT_MS = 100

fun newDeadLetterErrorHandler(
    template: KafkaOperations<ByteArray, ByteArray>,
    metrics: RecordingProcessingMetrics,
    backOffHandler: BackOffHandler,
    backOff: BackOffProperties = NO_JITTER_BACK_OFF,
    waitForSendResultTimeout: Duration = Duration.ofMillis(300),
    clock: Clock = DEAD_LETTER_CLOCK,
): DefaultErrorHandler =
    DeadLetterConfig().deadLetterErrorHandler(template, DEAD_LETTER_TOPIC, waitForSendResultTimeout, clock, metrics, backOff, backOffHandler)

fun ProducerRecord<ByteArray, ByteArray>.headerText(name: String): String? = headers().lastHeader(name)?.value()?.toString(Charsets.UTF_8)

class DeadLetterHarness(
    val backOffs: RecordingBackOffHandler = RecordingBackOffHandler(),
    backOff: BackOffProperties = NO_JITTER_BACK_OFF,
    clock: Clock = DEAD_LETTER_CLOCK,
    waitForSendResultTimeout: Duration = Duration.ofMillis(300),
) {
    val publications = CopyOnWriteArrayList<ProducerRecord<ByteArray, ByteArray>>()

    @Volatile
    var publishFailure: RuntimeException? = null

    val template = mockKafkaOperations()
    val metrics = RecordingProcessingMetrics()
    val consumer = mock(Consumer::class.java)
    val container = mock(MessageListenerContainer::class.java)

    val record: ConsumerRecord<Any, Any> =
        aRecord(ORIGINAL_VALUE, key = ORIGINAL_KEY).also {
            it.headers().add(RecordHeader("origem", "autorizador".toByteArray()))
        }

    val partition = TopicPartition(record.topic(), record.partition())

    val handler: DefaultErrorHandler = newDeadLetterErrorHandler(template, metrics, backOffs, backOff, waitForSendResultTimeout, clock)

    init {
        doAnswer { invocation ->
            val outbound = invocation.getArgument<ProducerRecord<ByteArray, ByteArray>>(0)
            val failure = publishFailure
            if (failure != null) {
                CompletableFuture.failedFuture<SendResult<ByteArray, ByteArray>>(failure)
            } else {
                publications += outbound
                CompletableFuture.completedFuture(SendResult(outbound, RecordMetadata(TopicPartition(outbound.topic(), 1), 0L, 0, 0L, 0, 0)))
            }
        }.`when`(template).send(anyProducerRecord())
    }

    fun deliver(cause: Exception): Boolean = deliver(record, cause)

    fun deliver(
        record: ConsumerRecord<Any, Any>,
        cause: Exception,
    ): Boolean =
        try {
            handler.handleRemaining(listenerFailed(cause), listOf(record), consumer, container)
            true
        } catch (signal: RuntimeException) {
            if (!isRedeliverySignal(signal)) throw signal
            false
        }

    fun singleDltRecord(): ProducerRecord<ByteArray, ByteArray> {
        assertEquals(1, publications.size, "publicacoes no DLT")
        return publications.single()
    }

    fun makeDltNeverAnswer() {
        val producerFactory = mock(ProducerFactory::class.java)
        doReturn(mapOf<String, Any>("delivery.timeout.ms" to SHORT_DELIVERY_TIMEOUT_MS)).`when`(producerFactory).configurationProperties
        doReturn(producerFactory).`when`(template).producerFactory
        doAnswer { CompletableFuture<SendResult<ByteArray, ByteArray>>() }.`when`(template).send(anyProducerRecord())
    }
}
