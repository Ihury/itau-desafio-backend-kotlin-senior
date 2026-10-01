package br.com.itau.challenge.balance.testing

import br.com.itau.challenge.balance.adapter.input.kafka.BackOffProperties
import br.com.itau.challenge.balance.adapter.input.kafka.DeadLetterConfig
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.mock
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

val NO_JITTER_BACK_OFF = BackOffProperties(initialMs = 500, maxMs = 30_000, jitterMs = 0)

val DEAD_LETTER_CLOCK: Clock = Clock.fixed(Instant.parse("2026-06-01T12:00:00Z"), ZoneOffset.UTC)

class DeadLetterHarness(
    val backOffHandler: BackOffHandler = RecordingBackOffHandler(),
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

    val handler: DefaultErrorHandler =
        DeadLetterConfig().deadLetterErrorHandler(template, DEAD_LETTER_TOPIC, waitForSendResultTimeout, clock, metrics, backOff, backOffHandler)

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
}
