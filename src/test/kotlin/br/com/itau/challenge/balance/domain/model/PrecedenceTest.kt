package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.testing.TransactionEventFixtures
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_TIMESTAMP_MICROS
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.HIGHEST_TRANSACTION_ID
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.LOWEST_TRANSACTION_ID
import org.junit.jupiter.api.Test
import java.util.Random
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class PrecedenceTest {
    private val baseMicros = DEFAULT_TIMESTAMP_MICROS
    private val lowId = LOWEST_TRANSACTION_ID
    private val highId = HIGHEST_TRANSACTION_ID

    private fun precedence(
        micros: Long,
        id: String,
    ) = Precedence(EventInstant.transactionTimestamp(micros), TransactionId.parse(id))

    private fun snapshotWith(precedence: Precedence): BalanceSnapshot =
        BalanceSnapshot.from(
            TransactionEventFixtures.transactionEvent(
                transactionId = precedence.transactionId.value,
                timestampMicros = precedence.timestamp.micros,
            ),
        )

    @Test
    fun `orders by numeric timestamp first`() {
        assertTrue(precedence(baseMicros + 1, lowId) > precedence(baseMicros, highId))
        assertTrue(precedence(baseMicros, highId) < precedence(baseMicros + 1, lowId))
    }

    @Test
    fun `ties on timestamp are broken by the lexicographic order of the canonical id`() {
        assertTrue(precedence(baseMicros, "a0000000-0000-4000-8000-000000000000") > precedence(baseMicros, "9fffffff-ffff-4fff-8fff-ffffffffffff"))
        assertTrue(precedence(baseMicros, lowId) < precedence(baseMicros, highId))
    }

    @Test
    fun `equal timestamp and id are the same event`() {
        assertEquals(0, precedence(baseMicros, lowId).compareTo(precedence(baseMicros, lowId)))
        assertEquals(precedence(baseMicros, lowId), precedence(baseMicros, lowId))
        assertNotEquals(precedence(baseMicros, lowId), precedence(baseMicros, highId))
    }

    @Test
    fun `precedence orders ids textually while java util UUID compares signed longs so ffff sorts before 0000`() {
        assertTrue(precedence(baseMicros, highId) > precedence(baseMicros, lowId))
        assertTrue(UUID.fromString(highId) < UUID.fromString(lowId))
    }

    @Test
    fun `order matches String compareTo of lowercase ids over 2000 random pairs and differs from UUID compareTo`() {
        val random = Random(42)

        fun randomUuid() = UUID(random.nextLong(), random.nextLong())

        var divergencesFromUuid = 0
        repeat(2000) {
            val a = randomUuid()
            val b = randomUuid()
            val expected = a.toString().compareTo(b.toString())
            val actual = precedence(baseMicros, a.toString()).compareTo(precedence(baseMicros, b.toString()))
            assertEquals(Integer.signum(expected), Integer.signum(actual), "divergiu para $a x $b")
            if (Integer.signum(a.compareTo(b)) != Integer.signum(actual)) divergencesFromUuid++
        }
        assertTrue(divergencesFromUuid > 0, "a armadilha do UUID.compareTo nao apareceu na amostra")
    }

    @Test
    fun `uppercase ids are normalized before comparing`() {
        assertEquals(precedence(baseMicros, highId), precedence(baseMicros, highId.uppercase()))
        assertEquals(0, precedence(baseMicros, highId.uppercase()).compareTo(precedence(baseMicros, highId)))
        assertTrue(precedence(baseMicros, highId.uppercase()) > precedence(baseMicros, lowId))
    }

    private val currentId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    private val greaterId = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

    private fun snapshotAt(
        micros: Long,
        id: String,
    ) = snapshotWith(precedence(micros, id))

    @Test
    fun `an event supersedes an absent snapshot`() {
        assertTrue(snapshotAt(baseMicros, currentId).supersedes(null))
    }

    @Test
    fun `a newer timestamp supersedes the current snapshot whatever the id`() {
        assertTrue(snapshotAt(baseMicros + 1, lowId).supersedes(snapshotAt(baseMicros, currentId)))
    }

    @Test
    fun `an older timestamp does not supersede the current snapshot whatever the id`() {
        assertFalse(snapshotAt(baseMicros - 1, highId).supersedes(snapshotAt(baseMicros, currentId)))
    }

    @Test
    fun `on a timestamp tie the greater id supersedes`() {
        assertTrue(snapshotAt(baseMicros, greaterId).supersedes(snapshotAt(baseMicros, currentId)))
    }

    @Test
    fun `on a timestamp tie the lower id does not supersede`() {
        assertFalse(snapshotAt(baseMicros, currentId).supersedes(snapshotAt(baseMicros, greaterId)))
    }

    @Test
    fun `the same timestamp and id do not supersede, it is a duplicate`() {
        assertFalse(snapshotAt(baseMicros, currentId).supersedes(snapshotAt(baseMicros, currentId)))
    }

    @Test
    fun `an event already superseded by a newer one does not supersede it again`() {
        assertFalse(snapshotAt(baseMicros, currentId).supersedes(snapshotAt(baseMicros + 1, lowId)))
    }
}
