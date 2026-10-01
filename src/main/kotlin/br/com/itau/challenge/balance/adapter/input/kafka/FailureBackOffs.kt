package br.com.itau.challenge.balance.adapter.input.kafka

import org.springframework.util.backoff.BackOff
import org.springframework.util.backoff.ExponentialBackOff
import org.springframework.util.backoff.FixedBackOff

internal object FailureBackOffs {
    private const val TRANSIENT_MULTIPLIER = 2.0
    private const val UNCLASSIFIED_INTERVAL_MS = 100L
    private const val UNCLASSIFIED_RETRIES = 2L
    private const val NO_WAIT_MS = 0L
    private const val NO_RETRIES = 0L

    fun transientFailure(settings: BackOffProperties): ExponentialBackOff =
        ExponentialBackOff(settings.initialMs, TRANSIENT_MULTIPLIER).apply {
            maxInterval = settings.maxMs
            jitter = settings.jitterMs
        }

    fun unclassifiedFailure(): FixedBackOff = FixedBackOff(UNCLASSIFIED_INTERVAL_MS, UNCLASSIFIED_RETRIES)

    fun permanentFailure(): FixedBackOff = FixedBackOff(NO_WAIT_MS, NO_RETRIES)

    fun forFailure(
        failure: Exception,
        settings: BackOffProperties,
    ): BackOff =
        when (FailureClassifier.classify(failure)) {
            FailureClass.PERMANENT -> permanentFailure()
            FailureClass.TRANSIENT -> transientFailure(settings)
            FailureClass.UNCLASSIFIED -> unclassifiedFailure()
        }
}
