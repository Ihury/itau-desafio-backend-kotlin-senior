package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.model.RejectionReason
import org.apache.kafka.common.header.Headers
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertNull

class RejectionHeadersTest {
    private val clock = Clock.fixed(Instant.parse("2026-06-01T12:34:56.789Z"), ZoneOffset.UTC)
    private val rejectionHeaders = RejectionHeaders(clock)

    private fun Headers.text(name: String): String? = lastHeader(name)?.value()?.toString(Charsets.UTF_8)

    @Test
    fun `the reason header carries the stable code and the detail carries only the field path`() {
        val headers = rejectionHeaders.of(RejectionReason.INVALID_CURRENCY, "transaction.currency")

        assertEquals("invalid_currency", headers.text("x-rejection-reason"))
        assertEquals("transaction.currency", headers.text("x-rejection-detail"))
    }

    @Test
    fun `the detail header is absent when there is no field path`() {
        val headers = rejectionHeaders.of(RejectionReason.MALFORMED_PAYLOAD, null)

        assertEquals("malformed_payload", headers.text("x-rejection-reason"))
        assertNull(headers.lastHeader("x-rejection-detail"))
    }

    @Test
    fun `the rejection instant is ISO 8601 UTC from the injected clock`() {
        val headers = rejectionHeaders.of(RejectionReason.UNPROCESSABLE_EVENT, null)

        assertEquals("2026-06-01T12:34:56.789Z", headers.text("x-rejected-at"))
    }

    @Test
    fun `there is exactly one header of each kind and nothing else`() {
        val headers = rejectionHeaders.of(RejectionReason.MISSING_FIELD, "account.id")

        assertEquals(listOf("x-rejection-reason", "x-rejection-detail", "x-rejected-at"), headers.map { it.key() })
    }

    @Test
    fun `every reason of the catalog is published by its code`() {
        RejectionReason.entries.forEach { reason ->
            assertEquals(reason.code, rejectionHeaders.of(reason, null).text("x-rejection-reason"))
        }
    }
}
