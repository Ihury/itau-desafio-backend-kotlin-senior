package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.testing.DeadLetterHarness
import br.com.itau.challenge.balance.testing.headerText
import org.junit.jupiter.api.Test
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class DeadLetterPublicationFailureTest {
    private val dlt = DeadLetterHarness()

    @Test
    fun `rejected is counted exactly once and only after the dlt confirms`() {
        dlt.publishFailure = RuntimeException("broker fora")
        assertFalse(dlt.deliver(InvalidEventException(RejectionReason.INVALID_IDENTIFIER, "account.id")))
        assertEquals(emptyList(), dlt.metrics.outcomes, "nada e contado enquanto o DLT nao confirma")

        dlt.publishFailure = null
        assertTrue(dlt.deliver(InvalidEventException(RejectionReason.INVALID_IDENTIFIER, "account.id")))

        assertEquals(listOf("rejected(invalid_identifier)"), dlt.metrics.outcomes)
    }

    @Test
    fun `a failing dlt publication leaves the record unconfirmed and redelivered, counts the failure and keeps the container alive`() {
        dlt.publishFailure = RuntimeException("broker fora")

        repeat(5) { assertFalse(dlt.deliver(InvalidEventException(RejectionReason.MISSING_FIELD, "account.id")), "tentativa ${it + 1}") }

        assertEquals(5, dlt.metrics.dltPublishFailures)
        assertEquals(emptyList(), dlt.metrics.outcomes)
        assertEquals(0, dlt.publications.size)
        verify(dlt.consumer, times(5)).seek(dlt.partition, dlt.record.offset())
        verify(dlt.consumer, never()).commitSync()
        verify(dlt.consumer, never()).commitAsync()
    }

    @Test
    fun `a dlt publication that never answers times out at the send result timeout, is not confirmed and counts the failure`() {
        dlt.makeDltNeverAnswer()

        val started = System.nanoTime()
        assertFalse(dlt.deliver(InvalidEventException(RejectionReason.MALFORMED_PAYLOAD)))
        val elapsedMillis = Duration.ofNanos(System.nanoTime() - started).toMillis()

        assertTrue(elapsedMillis in 250..3000, "espera limitada pelo waitForSendResultTimeout (300 ms no teste): $elapsedMillis ms")
        assertEquals(1, dlt.metrics.dltPublishFailures)
        assertEquals(emptyList(), dlt.metrics.outcomes)
    }

    @Test
    fun `a dlt failure on the last delivery of an unclassified failure keeps the record and Spring restarts the three deliveries before isolating it`() {
        assertFalse(dlt.deliver(IllegalStateException("x")))
        assertFalse(dlt.deliver(IllegalStateException("x")))
        dlt.publishFailure = RuntimeException("broker fora")
        assertFalse(dlt.deliver(IllegalStateException("x")), "3a entrega: o DLT falhou, o registro e reentregue")
        assertEquals(1, dlt.metrics.dltPublishFailures)
        assertEquals(emptyList(), dlt.metrics.outcomes)

        dlt.publishFailure = null
        assertFalse(dlt.deliver(IllegalStateException("x")), "contagem reiniciada apos a falha de recuperacao: 1a de 3 novas entregas")
        assertFalse(dlt.deliver(IllegalStateException("x")), "2a de 3 novas entregas")
        assertTrue(dlt.deliver(IllegalStateException("x")), "com o DLT de volta, o registro e isolado")

        assertEquals("unprocessable_event", dlt.singleDltRecord().headerText("x-rejection-reason"))
        assertEquals(listOf("rejected(unprocessable_event)"), dlt.metrics.outcomes)
    }
}
