package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.RejectionReason
import org.springframework.kafka.listener.ListenerExecutionFailedException

enum class FailureClass {
    PERMANENT,
    TRANSIENT,
    UNCLASSIFIED,
}

data class Rejection(
    val reason: RejectionReason,
    val fieldPath: String?,
)

object FailureClassifier {
    fun classify(failure: Throwable): FailureClass =
        when (unwrap(failure)) {
            is InvalidEventException -> FailureClass.PERMANENT
            is BalanceStoreUnavailableException -> FailureClass.TRANSIENT
            else -> FailureClass.UNCLASSIFIED
        }

    fun rejectionOf(failure: Throwable?): Rejection =
        when (val root = failure?.let(::unwrap)) {
            is InvalidEventException -> Rejection(root.reason, root.fieldPath)
            else -> Rejection(RejectionReason.UNPROCESSABLE_EVENT, null)
        }

    fun unwrap(failure: Throwable): Throwable {
        var current = failure
        while (current is ListenerExecutionFailedException) {
            current = current.cause ?: return current
        }
        return current
    }
}
