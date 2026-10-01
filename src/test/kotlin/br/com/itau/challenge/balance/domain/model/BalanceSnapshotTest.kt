package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_ACCOUNT_CREATED_AT_MICROS
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_OWNER_ID
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_TIMESTAMP_MICROS
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.HIGHEST_TRANSACTION_ID
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.LOWEST_TRANSACTION_ID
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.transactionEvent
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.reflect.full.memberProperties
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class BalanceSnapshotTest {
    private val baseMicros = DEFAULT_TIMESTAMP_MICROS
    private val lowId = LOWEST_TRANSACTION_ID
    private val highId = HIGHEST_TRANSACTION_ID

    @Test
    fun `from copies owner, account status, balance and created_at from the same event`() {
        val event =
            transactionEvent(
                ownerId = DEFAULT_OWNER_ID,
                accountStatus = AccountStatus.DISABLED,
                balanceAmount = "183.12",
                balanceCurrency = "BRL",
                accountCreatedAtMicros = DEFAULT_ACCOUNT_CREATED_AT_MICROS,
            )

        val snapshot = BalanceSnapshot.from(event)

        assertEquals(event.account.id, snapshot.accountId)
        assertEquals(event.account.owner, snapshot.ownerId)
        assertEquals(AccountStatus.DISABLED, snapshot.status)
        assertEquals(event.account.balance, snapshot.balance)
        assertEquals(EventInstant.accountCreatedAt(DEFAULT_ACCOUNT_CREATED_AT_MICROS), snapshot.accountCreatedAt)
    }

    @Test
    fun `precedence comes from the transaction timestamp and id`() {
        val snapshot = BalanceSnapshot.from(transactionEvent(transactionId = highId.uppercase(), timestampMicros = baseMicros))

        assertEquals(Precedence(EventInstant.transactionTimestamp(baseMicros), TransactionId.parse(highId)), snapshot.precedence)
    }

    @Test
    fun `supersedes an absent snapshot`() {
        assertTrue(BalanceSnapshot.from(transactionEvent()).supersedes(null))
    }

    @Test
    fun `supersedes compares the precedence key`() {
        val current = BalanceSnapshot.from(transactionEvent(transactionId = lowId, timestampMicros = baseMicros))

        assertTrue(BalanceSnapshot.from(transactionEvent(transactionId = lowId, timestampMicros = baseMicros + 1)).supersedes(current))
        assertFalse(BalanceSnapshot.from(transactionEvent(transactionId = highId, timestampMicros = baseMicros - 1)).supersedes(current))
        assertTrue(BalanceSnapshot.from(transactionEvent(transactionId = highId, timestampMicros = baseMicros)).supersedes(current))
        assertFalse(
            BalanceSnapshot.from(transactionEvent(transactionId = lowId, timestampMicros = baseMicros)).supersedes(
                BalanceSnapshot.from(transactionEvent(transactionId = highId, timestampMicros = baseMicros)),
            ),
        )
        assertFalse(BalanceSnapshot.from(transactionEvent(transactionId = lowId, timestampMicros = baseMicros)).supersedes(current))
    }

    @Test
    fun `declined transactions and disabled accounts supersede like any other event`() {
        val current = BalanceSnapshot.from(transactionEvent(timestampMicros = baseMicros))

        val declined = transactionEvent(timestampMicros = baseMicros + 1, transactionStatus = TransactionStatus.DECLINED)
        assertTrue(BalanceSnapshot.from(declined).supersedes(current))

        val disabled = transactionEvent(timestampMicros = baseMicros + 1, accountStatus = AccountStatus.DISABLED)
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
