package br.com.itau.challenge.balance.domain.model

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class RejectionReasonTest {
    private val expectedCodes =
        listOf(
            "malformed_payload",
            "missing_field",
            "invalid_identifier",
            "invalid_value",
            "invalid_currency",
            "invalid_timestamp",
            "unknown_domain_value",
            "unprocessable_event",
        )

    @Test
    fun `exposes exactly the eight stable codes of the rejection catalog`() {
        assertEquals(expectedCodes.toSet(), RejectionReason.entries.map { it.code }.toSet())
        assertEquals(expectedCodes.size, RejectionReason.entries.size)
    }

    @Test
    fun `codes are unique`() {
        assertEquals(RejectionReason.entries.size, RejectionReason.entries.map { it.code }.distinct().size)
    }

    @Test
    fun `fromCode is the inverse of code`() {
        RejectionReason.entries.forEach { assertEquals(it, RejectionReason.fromCode(it.code)) }
    }

    @Test
    fun `fromCode returns null for an unknown code`() {
        assertNull(RejectionReason.fromCode("nao_existe"))
        assertNull(RejectionReason.fromCode("MALFORMED_PAYLOAD"))
    }
}
