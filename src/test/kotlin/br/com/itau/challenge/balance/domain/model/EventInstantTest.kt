package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class EventInstantTest {
    private val year2000 = 946684800000000L
    private val year1900 = -2208988800000000L

    private fun assertInvalidTimestamp(block: () -> Unit) {
        val error = assertFailsWith<InvalidEventException> { block() }
        assertEquals(RejectionReason.INVALID_TIMESTAMP, error.reason)
    }

    @Test
    fun `transaction timestamp accepts exactly 2000-01-01 and rejects anything before`() {
        assertEquals(year2000, EventInstant.transactionTimestamp(year2000).micros)
        assertInvalidTimestamp { EventInstant.transactionTimestamp(year2000 - 1) }
        assertInvalidTimestamp { EventInstant.transactionTimestamp(-1) }
        assertInvalidTimestamp { EventInstant.transactionTimestamp(0) }
    }

    @Test
    fun `a persisted instant is rebuilt as is, without the plausibility minimums`() {
        assertEquals(1751749453433123L, EventInstant.fromPersisted(1751749453433123L).micros)
        assertEquals(year2000 - 1, EventInstant.fromPersisted(year2000 - 1).micros)
        assertEquals(year1900 - 1, EventInstant.fromPersisted(year1900 - 1).micros)
    }

    @Test
    fun `transaction timestamp detects seconds and milliseconds as the wrong unit`() {
        assertInvalidTimestamp { EventInstant.transactionTimestamp(1751749453433) }
        assertInvalidTimestamp { EventInstant.transactionTimestamp(1751749453) }
    }

    @Test
    fun `account created_at accepts 1900-01-01 as minimum, including accounts before 1970 and before 2000`() {
        assertEquals(year1900, EventInstant.accountCreatedAt(year1900).micros)
        assertEquals(899251200000000, EventInstant.accountCreatedAt(899251200000000).micros)
        assertEquals(-1_000_000_000_000_000, EventInstant.accountCreatedAt(-1_000_000_000_000_000).micros)
        assertEquals(0, EventInstant.accountCreatedAt(0).micros)
        assertInvalidTimestamp { EventInstant.accountCreatedAt(year1900 - 1) }
    }

    @Test
    fun `the minimum is a parameter of the role`() {
        val minimum = Instant.parse("2010-01-01T00:00:00Z")
        val micros = 1262304000000000L
        assertEquals(micros, EventInstant.transactionTimestamp(micros, minimum).micros)
        assertInvalidTimestamp { EventInstant.transactionTimestamp(micros - 1, minimum) }
        assertEquals(micros, EventInstant.accountCreatedAt(micros, minimum).micros)
        assertInvalidTimestamp { EventInstant.accountCreatedAt(micros - 1, minimum) }
    }

    @Test
    fun `converts to Instant without losing microseconds`() {
        assertEquals(
            Instant.parse("2025-07-05T21:04:13.433123Z"),
            EventInstant.transactionTimestamp(1751749453433123).toInstant(),
        )
        assertEquals(
            Instant.parse("2025-07-05T21:04:13.433Z"),
            EventInstant.transactionTimestamp(1751749453433000).toInstant(),
        )
    }

    @Test
    fun `converts negative microseconds with floor semantics`() {
        assertEquals(Instant.parse("1969-12-31T23:59:59.999999Z"), EventInstant.accountCreatedAt(-1).toInstant())
        assertEquals(Instant.parse("1969-12-31T23:59:59Z"), EventInstant.accountCreatedAt(-1_000_000).toInstant())
        assertEquals(Instant.parse("1969-12-31T23:59:58.999999Z"), EventInstant.accountCreatedAt(-1_000_001).toInstant())
        assertEquals(Instant.parse("1900-01-01T00:00:00Z"), EventInstant.accountCreatedAt(year1900).toInstant())
        assertEquals(Instant.parse("1970-01-01T00:00:00Z"), EventInstant.accountCreatedAt(0).toInstant())
    }

    @Test
    fun `orders by microseconds`() {
        val early = EventInstant.transactionTimestamp(year2000)
        val late = EventInstant.transactionTimestamp(year2000 + 1)

        assertTrue(early < late)
        assertTrue(late > early)
        assertEquals(early, EventInstant.transactionTimestamp(year2000))
        assertEquals(listOf(early, late), listOf(late, early).sorted())
        assertEquals(late, maxOf(early, late))
    }

    @Test
    fun `toString prints the microseconds`() {
        assertEquals(year2000.toString(), EventInstant.transactionTimestamp(year2000).toString())
    }
}
