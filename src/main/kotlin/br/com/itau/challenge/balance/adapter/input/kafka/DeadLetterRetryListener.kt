package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.adapter.input.MdcKeys
import br.com.itau.challenge.balance.adapter.input.logStoreUnavailable
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.port.output.ConsumerFailureMetrics
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.RetryListener

internal class DeadLetterRetryListener(
    private val metrics: ConsumerFailureMetrics,
) : RetryListener {
    override fun failedDelivery(
        record: ConsumerRecord<*, *>,
        exception: Exception?,
        deliveryAttempt: Int,
    ) {
        val failure = exception?.let { FailureClassifier.unwrap(it) }
        withMdc(MdcKeys.CORRELATION_ID to record.coordinates()) {
            when (failure) {
                is BalanceStoreUnavailableException -> storeUnavailable(record, failure, deliveryAttempt)
                is InvalidEventException -> log.debug("invalid event {} reason={}", record.coordinates(), failure.reason.code)
                else -> unclassified(record, failure, deliveryAttempt)
            }
        }
    }

    override fun recovered(
        record: ConsumerRecord<*, *>,
        exception: Exception?,
    ) {
        val rejection = FailureClassifier.rejectionOf(exception)
        metrics.rejected(rejection.reason)
        withMdc(MdcKeys.CORRELATION_ID to record.coordinates()) {
            log.warn("message isolated in the dlt {} reason={} detail={}", record.coordinates(), rejection.reason.code, rejection.fieldPath)
        }
    }

    override fun recoveryFailed(
        record: ConsumerRecord<*, *>,
        original: Exception?,
        failure: Exception,
    ) {
        metrics.dltPublishFailed()
        withMdc(MdcKeys.CORRELATION_ID to record.coordinates()) {
            log.error("dlt publication failed, record not confirmed and will be redelivered {} exception={}", record.coordinates(), failure.javaClass.name)
        }
    }

    private fun storeUnavailable(
        record: ConsumerRecord<*, *>,
        failure: BalanceStoreUnavailableException,
        deliveryAttempt: Int,
    ) {
        metrics.backpressure(failure.failureCause)
        log.logStoreUnavailable(failure, "store unavailable, container paused for the back off {} attempt={} {}", record.coordinates(), deliveryAttempt, failure.logDescription())
    }

    private fun unclassified(
        record: ConsumerRecord<*, *>,
        failure: Throwable?,
        deliveryAttempt: Int,
    ) {
        log.error(
            "unclassified failure {} attempt={} exception={} at={}",
            record.coordinates(),
            deliveryAttempt,
            failure?.javaClass?.name,
            failure?.stackTrace?.firstOrNull(),
        )
    }

    private companion object {
        private val log = LoggerFactory.getLogger(DeadLetterRetryListener::class.java)
    }
}
