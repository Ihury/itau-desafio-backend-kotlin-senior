package br.com.itau.challenge.balance.domain.exception

import br.com.itau.challenge.balance.domain.model.RejectionReason
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class InvalidEventExceptionTest {
    @Test
    fun `carries the reason and an optional field path`() {
        val withoutDetail = InvalidEventException(RejectionReason.MALFORMED_PAYLOAD)
        assertEquals(RejectionReason.MALFORMED_PAYLOAD, withoutDetail.reason)
        assertNull(withoutDetail.fieldPath)

        val withDetail = InvalidEventException(RejectionReason.INVALID_CURRENCY, "transaction.currency")
        assertEquals("transaction.currency", withDetail.fieldPath)
    }

    @Test
    fun `withFieldPath returns a copy with the path and keeps the reason`() {
        val original = InvalidEventException(RejectionReason.INVALID_VALUE)
        val copy = original.withFieldPath("account.balance.amount")

        assertEquals(RejectionReason.INVALID_VALUE, copy.reason)
        assertEquals("account.balance.amount", copy.fieldPath)
        assertNull(original.fieldPath)
    }

    @Test
    fun `message is only the reason code and never a value`() {
        val error = InvalidEventException(RejectionReason.INVALID_CURRENCY, "transaction.currency")
        assertEquals("invalid_currency", error.message)
    }
}
