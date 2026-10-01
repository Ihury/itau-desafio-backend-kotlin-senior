package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.BalanceStoreRejectedException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.testing.listenerFailed
import org.junit.jupiter.api.Test
import org.springframework.kafka.listener.ListenerExecutionFailedException
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertSame

class FailureClassifierTest {
    @Test
    fun `an invalid event is permanent`() {
        assertEquals(FailureClass.PERMANENT, FailureClassifier.classify(InvalidEventException(RejectionReason.INVALID_VALUE)))
    }

    @Test
    fun `an unavailable store is transient for every failure cause`() {
        StoreFailureCause.entries.forEach { cause ->
            assertEquals(FailureClass.TRANSIENT, FailureClassifier.classify(BalanceStoreUnavailableException(cause)), cause.name)
        }
    }

    @Test
    fun `anything else is unclassified, including a store rejection and internal failures`() {
        listOf(
            BalanceStoreRejectedException(),
            IllegalStateException("x"),
            NullPointerException(),
            IllegalArgumentException("x"),
            ClassCastException("x"),
            RuntimeException("x"),
        ).forEach { failure ->
            assertEquals(FailureClass.UNCLASSIFIED, FailureClassifier.classify(failure), failure.javaClass.simpleName)
        }
    }

    @Test
    fun `exceptions wrapped by the listener adapter are unwrapped before classifying`() {
        assertEquals(FailureClass.PERMANENT, FailureClassifier.classify(listenerFailed(InvalidEventException(RejectionReason.MISSING_FIELD))))
        assertEquals(FailureClass.TRANSIENT, FailureClassifier.classify(listenerFailed(BalanceStoreUnavailableException(StoreFailureCause.TIMEOUT))))
        assertEquals(FailureClass.UNCLASSIFIED, FailureClassifier.classify(listenerFailed(IllegalStateException("x"))))
        assertEquals(FailureClass.PERMANENT, FailureClassifier.classify(listenerFailed(listenerFailed(InvalidEventException(RejectionReason.INVALID_TIMESTAMP)))))
    }

    @Test
    fun `a wrapper without cause is unclassified`() {
        assertEquals(FailureClass.UNCLASSIFIED, FailureClassifier.classify(ListenerExecutionFailedException("Listener failed")))
    }

    @Test
    fun `the rejection of a permanent failure keeps the reason and the field path`() {
        val rejection = FailureClassifier.rejectionOf(listenerFailed(InvalidEventException(RejectionReason.INVALID_CURRENCY, "transaction.currency")))

        assertEquals(RejectionReason.INVALID_CURRENCY, rejection.reason)
        assertEquals("transaction.currency", rejection.fieldPath)
    }

    @Test
    fun `any non permanent failure is isolated as unprocessable event without detail`() {
        val rejection = FailureClassifier.rejectionOf(listenerFailed(IllegalStateException("x")))

        assertEquals(RejectionReason.UNPROCESSABLE_EVENT, rejection.reason)
        assertNull(rejection.fieldPath)
    }

    @Test
    fun `unwrap returns the innermost non wrapper exception`() {
        val inner = IllegalStateException("x")

        assertSame(inner, FailureClassifier.unwrap(listenerFailed(listenerFailed(inner))))
        assertSame(inner, FailureClassifier.unwrap(inner))
    }
}
