package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.port.output.ConsumerFailureMetrics
import org.apache.kafka.common.TopicPartition
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.BackOffHandler
import org.springframework.kafka.listener.CommonErrorHandler
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd
import org.springframework.kafka.listener.DefaultErrorHandler
import java.time.Clock
import java.time.Duration

@Configuration
@EnableConfigurationProperties(DeadLetterProperties::class)
class DeadLetterConfig {
    @Bean
    fun deadLetterKafkaTemplate(producerFactory: ProducerFactory<*, *>): KafkaTemplate<ByteArray, ByteArray> {
        @Suppress("UNCHECKED_CAST")
        return KafkaTemplate(producerFactory as ProducerFactory<ByteArray, ByteArray>)
    }

    @Bean
    fun kafkaErrorHandler(
        deadLetterKafkaTemplate: KafkaTemplate<ByteArray, ByteArray>,
        @Value($$"${balance.events.dlt-topic}") dltTopic: String,
        properties: DeadLetterProperties,
        clock: Clock,
        metrics: ConsumerFailureMetrics,
        backOff: BackOffProperties,
        containerPausingBackOffHandler: BackOffHandler,
    ): CommonErrorHandler =
        deadLetterErrorHandler(deadLetterKafkaTemplate, dltTopic, properties.waitForSendResultTimeout, clock, metrics, backOff, containerPausingBackOffHandler)

    internal fun deadLetterErrorHandler(
        template: KafkaOperations<ByteArray, ByteArray>,
        dltTopic: String,
        waitForSendResultTimeout: Duration,
        clock: Clock,
        metrics: ConsumerFailureMetrics,
        backOff: BackOffProperties,
        backOffHandler: BackOffHandler,
    ): DefaultErrorHandler {
        val recoverer = deadLetterRecoverer(template, dltTopic, waitForSendResultTimeout, RejectionHeaders(clock))
        val neverExhaustingBackOff = FailureBackOffs.transientFailure(backOff)
        val handler = DefaultErrorHandler(recoverer, neverExhaustingBackOff, backOffHandler)
        handler.setClassifications(NOT_RETRYABLE, RETRYABLE_BY_DEFAULT)
        handler.setBackOffFunction { _, failure -> FailureBackOffs.forFailure(failure, backOff) }
        handler.setRetryListeners(DeadLetterRetryListener(metrics))
        return handler
    }

    private fun deadLetterRecoverer(
        template: KafkaOperations<ByteArray, ByteArray>,
        dltTopic: String,
        waitForSendResultTimeout: Duration,
        rejectionHeaders: RejectionHeaders,
    ): DeadLetterPublishingRecoverer {
        val recoverer = DeadLetterPublishingRecoverer(template) { _, _ -> TopicPartition(dltTopic, PARTITION_CHOSEN_BY_PARTITIONER) }
        recoverer.setHeadersFunction { _, failure -> rejectionHeaders.of(FailureClassifier.rejectionOf(failure)) }
        recoverer.excludeHeader(*EXCEPTION_HEADERS)
        recoverer.setFailIfSendResultIsError(true)
        recoverer.setWaitForSendResultTimeout(waitForSendResultTimeout)
        recoverer.setTimeoutBuffer(NO_TIMEOUT_BUFFER_MS)
        return recoverer
    }

    private companion object {
        const val PARTITION_CHOSEN_BY_PARTITIONER = -1
        const val NO_TIMEOUT_BUFFER_MS = 0L
        const val RETRYABLE_BY_DEFAULT = true
        val NOT_RETRYABLE = mapOf<Class<out Throwable>, Boolean>(InvalidEventException::class.java to false)
        val EXCEPTION_HEADERS = arrayOf(HeadersToAdd.EXCEPTION, HeadersToAdd.EX_CAUSE, HeadersToAdd.EX_MSG, HeadersToAdd.EX_STACKTRACE)
    }
}
