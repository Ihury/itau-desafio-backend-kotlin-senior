package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.model.TransactionEventFixtures.transactionEvent
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.reflect.full.memberProperties
import kotlin.test.assertTrue

class BalanceSnapshotTest {
    private val t = 1751749453433000L
    private val lowId = "00000000-0000-4000-8000-000000000001"
    private val highId = "ffffffff-ffff-4fff-8fff-ffffffffff01"

    @Test
    fun `from copies owner, account status, balance and created_at from the same event`() {
        val event =
            transactionEvent(
                ownerId = "315e3cfe-f4af-4cd2-b298-a449e614349a",
                accountStatus = AccountStatus.DISABLED,
                balanceAmount = "183.12",
                balanceCurrency = "BRL",
                accountCreatedAtMicros = 1634874339000000L,
            )

        val snapshot = BalanceSnapshot.from(event)

        assertEquals(event.account.id, snapshot.accountId)
        assertEquals(event.account.owner, snapshot.ownerId)
        assertEquals(AccountStatus.DISABLED, snapshot.status)
        assertEquals(event.account.balance, snapshot.balance)
        assertEquals(EventInstant.accountCreatedAt(1634874339000000L), snapshot.accountCreatedAt)
    }

    @Test
    fun `precedence comes from the transaction timestamp and id`() {
        val snapshot = BalanceSnapshot.from(transactionEvent(transactionId = highId.uppercase(), timestampMicros = t))

        assertEquals(Precedence(EventInstant.transactionTimestamp(t), TransactionId.parse(highId)), snapshot.precedence)
    }

    @Test
    fun `supersedes with an absent current snapshot`() {
        assertTrue(BalanceSnapshot.from(transactionEvent()).supersedes(null))
    }

    @Test
    fun `supersedes compares the precedence key`() {
        val current = BalanceSnapshot.from(transactionEvent(transactionId = lowId, timestampMicros = t))

        assertTrue(BalanceSnapshot.from(transactionEvent(transactionId = lowId, timestampMicros = t + 1)).supersedes(current))
        assertFalse(BalanceSnapshot.from(transactionEvent(transactionId = highId, timestampMicros = t - 1)).supersedes(current))
        assertTrue(BalanceSnapshot.from(transactionEvent(transactionId = highId, timestampMicros = t)).supersedes(current))
        assertFalse(
            BalanceSnapshot.from(transactionEvent(transactionId = lowId, timestampMicros = t)).supersedes(
                BalanceSnapshot.from(transactionEvent(transactionId = highId, timestampMicros = t)),
            ),
        )
        assertFalse(BalanceSnapshot.from(transactionEvent(transactionId = lowId, timestampMicros = t)).supersedes(current))
    }

    @Test
    fun `declined transactions and disabled accounts supersede like any other event`() {
        val current = BalanceSnapshot.from(transactionEvent(timestampMicros = t))

        val declined = transactionEvent(timestampMicros = t + 1, transactionStatus = TransactionStatus.DECLINED)
        assertTrue(BalanceSnapshot.from(declined).supersedes(current))

        val disabled = transactionEvent(timestampMicros = t + 1, accountStatus = AccountStatus.DISABLED)
        assertTrue(BalanceSnapshot.from(disabled).supersedes(current))
    }

    @Test
    fun `the snapshot keeps no transaction type, amount or status`() {
        val names = BalanceSnapshot::class.memberProperties.map { it.name }.toSet()

        assertEquals(
            setOf("accountId", "ownerId", "status", "balance", "accountCreatedAt", "precedence"),
            names,
        )
    }

    @Test
    fun `balance currency is independent of the transaction currency`() {
        val event =
            transactionEvent(
                transactionCurrency = "BRL",
                transactionAmount = "10.00",
                balanceCurrency = "USD",
                balanceAmount = "5.55",
            )

        val snapshot = BalanceSnapshot.from(event)

        assertEquals(CurrencyCode.parse("USD"), snapshot.balance.currency)
        assertEquals(0, BigDecimal("5.55").compareTo(snapshot.balance.amount))
    }
}
