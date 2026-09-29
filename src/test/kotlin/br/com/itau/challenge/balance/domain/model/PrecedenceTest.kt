package br.com.itau.challenge.balance.domain.model

import org.junit.jupiter.api.Test
import java.util.Random
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class PrecedenceTest {
    private val t = 1751749453433000L
    private val lowId = "00000000-0000-4000-8000-000000000001"
    private val highId = "ffffffff-ffff-4fff-8fff-ffffffffff01"

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
        assertTrue(precedence(t + 1, lowId) > precedence(t, highId))
        assertTrue(precedence(t, highId) < precedence(t + 1, lowId))
    }

    @Test
    fun `ties on timestamp are broken by the lexicographic order of the canonical id`() {
        assertTrue(precedence(t, "a0000000-0000-4000-8000-000000000000") > precedence(t, "9fffffff-ffff-4fff-8fff-ffffffffffff"))
        assertTrue(precedence(t, lowId) < precedence(t, highId))
    }

    @Test
    fun `equal timestamp and id are the same event`() {
        assertEquals(0, precedence(t, lowId).compareTo(precedence(t, lowId)))
        assertEquals(precedence(t, lowId), precedence(t, lowId))
        assertNotEquals(precedence(t, lowId), precedence(t, highId))
    }

    @Test
    fun `the UUID compareTo trap - signed longs diverge from the textual order`() {
        assertTrue(precedence(t, highId) > precedence(t, lowId))
        // java.util.UUID compara longs COM sinal: ffff... vira negativo e ordena antes de 0000...
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
            val actual = precedence(t, a.toString()).compareTo(precedence(t, b.toString()))
            assertEquals(Integer.signum(expected), Integer.signum(actual), "divergiu para $a x $b")
            if (Integer.signum(a.compareTo(b)) != Integer.signum(actual)) divergencesFromUuid++
        }
        assertTrue(divergencesFromUuid > 0, "a armadilha do UUID.compareTo nao apareceu na amostra")
    }

    @Test
    fun `uppercase ids are normalized before comparing`() {
        assertEquals(precedence(t, highId), precedence(t, highId.uppercase()))
        assertEquals(0, precedence(t, highId.uppercase()).compareTo(precedence(t, highId)))
        assertTrue(precedence(t, highId.uppercase()) > precedence(t, lowId))
    }

    @Test
    fun `supersedes follows the seven rows of the precedence table`() {
        val a = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        val b = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        val current = snapshotWith(precedence(t, a))

        // ausente -> processed
        assertTrue(snapshotWith(precedence(t, a)).supersedes(null))
        // (t, A) x (t+1, X) -> processed
        assertTrue(snapshotWith(precedence(t + 1, lowId)).supersedes(current))
        // (t, A) x (t-1, X) -> obsolete
        assertFalse(snapshotWith(precedence(t - 1, highId)).supersedes(current))
        // (t, A) x (t, B) com B > A -> processed
        assertTrue(snapshotWith(precedence(t, b)).supersedes(current))
        // (t, B) x (t, A) com A < B -> obsolete
        assertFalse(current.supersedes(snapshotWith(precedence(t, b))))
        // (t, A) x (t, A) -> duplicate (nao supersede)
        assertFalse(snapshotWith(precedence(t, a)).supersedes(current))
        // (t, A) reentregue depois de superado por (t+1, X) -> obsolete
        assertFalse(current.supersedes(snapshotWith(precedence(t + 1, lowId))))
    }
}
