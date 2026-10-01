package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.BalanceStoreRejectedException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.testing.DeadLetterHarness
import br.com.itau.challenge.balance.testing.ORIGINAL_VALUE
import br.com.itau.challenge.balance.testing.anyProducerRecord
import br.com.itau.challenge.balance.testing.headerText
import org.junit.jupiter.api.Test
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DeadLetterRetryPolicyTest {
    private val dlt = DeadLetterHarness()

    @Test
    fun `an unclassified failure gets three deliveries and then goes to the dlt as unprocessable event`() {
        assertFalse(dlt.deliver(IllegalStateException("defeito")), "1a entrega")
        assertEquals(0, dlt.publications.size)
        assertFalse(dlt.deliver(IllegalStateException("defeito")), "2a entrega")
        assertEquals(0, dlt.publications.size)
        assertTrue(dlt.deliver(IllegalStateException("defeito")), "3a entrega vai ao DLT")

        assertEquals(listOf(100L, 100L), dlt.backOffs.intervals)
        val outbound = dlt.singleDltRecord()
        assertEquals("unprocessable_event", outbound.headerText("x-rejection-reason"))
        assertNull(outbound.headers().lastHeader("x-rejection-detail"))
        assertEquals(listOf("rejected(unprocessable_event)"), dlt.metrics.outcomes)
        assertContentEquals(ORIGINAL_VALUE, outbound.value())
    }

    @Test
    fun `a store rejection is unclassified and follows the same three deliveries`() {
        assertFalse(dlt.deliver(BalanceStoreRejectedException()))
        assertFalse(dlt.deliver(BalanceStoreRejectedException()))
        assertTrue(dlt.deliver(BalanceStoreRejectedException()))

        assertEquals("unprocessable_event", dlt.singleDltRecord().headerText("x-rejection-reason"))
    }

    @Test
    fun `failures the spring default treats as fatal are also given three deliveries`() {
        assertFalse(dlt.deliver(ClassCastException("x")))
        assertFalse(dlt.deliver(ClassCastException("x")))
        assertTrue(dlt.deliver(ClassCastException("x")))

        assertEquals("unprocessable_event", dlt.singleDltRecord().headerText("x-rejection-reason"))
    }

    @Test
    fun `a failure the spring default treats as fatal is still retried, only the invalid event is not retryable`() {
        assertFalse(dlt.deliver(ClassCastException("x")))
        assertEquals(listOf(100L), dlt.backOffs.intervals)
    }

    @Test
    fun `a transient failure never reaches the recoverer however many times it repeats`() {
        StoreFailureCause.entries.forEach { cause ->
            repeat(50) { assertFalse(dlt.deliver(BalanceStoreUnavailableException(cause)), "$cause na tentativa ${it + 1}") }
        }

        assertEquals(0, dlt.publications.size)
        verify(dlt.template, never()).send(anyProducerRecord())
        assertEquals(emptyList(), dlt.metrics.outcomes)
        assertEquals(0, dlt.metrics.dltPublishFailures)
        assertTrue(dlt.backOffs.intervals.all { it in 1..30_000 })
    }

    @Test
    fun `the wait of a transient failure grows exponentially up to thirty seconds and never runs out`() {
        repeat(20) { dlt.deliver(BalanceStoreUnavailableException(StoreFailureCause.UNAVAILABLE)) }

        assertEquals(listOf(500L, 1000L, 2000L, 4000L, 8000L, 16000L, 30000L, 30000L), dlt.backOffs.intervals.take(8))
        assertEquals(30_000L, dlt.backOffs.intervals.last())
    }

    @Test
    fun `a transient failure followed by a permanent one for the same record is isolated at once`() {
        repeat(3) { assertFalse(dlt.deliver(BalanceStoreUnavailableException(StoreFailureCause.THROTTLED))) }
        assertTrue(dlt.deliver(InvalidEventException(RejectionReason.INVALID_VALUE, "transaction.amount")))

        assertEquals("invalid_value", dlt.singleDltRecord().headerText("x-rejection-reason"))
    }
}
