package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_ACCOUNT_CREATED_AT_MICROS
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_ACCOUNT_ID
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_OWNER_ID
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_TIMESTAMP_MICROS
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_TRANSACTION_ID
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.transactionEvent
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class TransactionEventModelTest {
    private fun assertUnknown(parse: (String) -> Any, vararg raws: String) {
        raws.forEach { raw ->
            val error = assertFailsWith<InvalidEventException>("aceitou '$raw'") { parse(raw) }
            assertEquals(RejectionReason.UNKNOWN_DOMAIN_VALUE, error.reason, "com '$raw'")
        }
    }

    @Test
    fun `TransactionType parses exact values only`() {
        assertEquals(TransactionType.CREDIT, TransactionType.parse("CREDIT"))
        assertEquals(TransactionType.DEBIT, TransactionType.parse("DEBIT"))
        assertUnknown(TransactionType::parse, "credit", "TRANSFER", "", " CREDIT")
    }

    @Test
    fun `TransactionStatus parses exact values only`() {
        assertEquals(TransactionStatus.APPROVED, TransactionStatus.parse("APPROVED"))
        assertEquals(TransactionStatus.DECLINED, TransactionStatus.parse("DECLINED"))
        assertUnknown(TransactionStatus::parse, "approved", "PENDING", "")
    }

    @Test
    fun `AccountStatus parses exact values only`() {
        assertEquals(AccountStatus.ENABLED, AccountStatus.parse("ENABLED"))
        assertEquals(AccountStatus.DISABLED, AccountStatus.parse("DISABLED"))
        assertUnknown(AccountStatus::parse, "enabled", "SUSPENDED", "")
    }

    @Test
    fun `transaction amount must be non-negative`() {
        assertEquals(BigDecimal("0"), transactionEvent(transactionAmount = "0").transaction.amount)
        assertEquals(BigDecimal("97.07"), transactionEvent(transactionAmount = "97.07").transaction.amount)

        val error = assertFailsWith<InvalidEventException> { transactionEvent(transactionAmount = "-0.01") }
        assertEquals(RejectionReason.INVALID_VALUE, error.reason)
    }

    @Test
    fun `transaction amount follows the precision and scale rules of money`() {
        val tooManyDigits = assertFailsWith<InvalidEventException> { transactionEvent(transactionAmount = "1".repeat(39)) }
        assertEquals(RejectionReason.INVALID_VALUE, tooManyDigits.reason)

        val tooMuchScale =
            assertFailsWith<InvalidEventException> { transactionEvent(transactionAmount = "0." + "0".repeat(38) + "1") }
        assertEquals(RejectionReason.INVALID_VALUE, tooMuchScale.reason)

        val hugeExponent = assertFailsWith<InvalidEventException> { transactionEvent(transactionAmount = "1E999999999") }
        assertEquals(RejectionReason.INVALID_VALUE, hugeExponent.reason)

        assertEquals(38, transactionEvent(transactionAmount = "1".repeat(38)).transaction.amount.precision())
    }

    @Test
    fun `the fixture event exposes the default transaction and account state`() {
        val event = transactionEvent()

        assertEquals(TransactionId.parse(DEFAULT_TRANSACTION_ID), event.transaction.id)
        assertEquals(AccountId.parse(DEFAULT_ACCOUNT_ID), event.account.id)
        assertEquals(OwnerId.parse(DEFAULT_OWNER_ID), event.account.owner)
        assertEquals(AccountStatus.ENABLED, event.account.status)
        assertEquals(TransactionType.CREDIT, event.transaction.type)
        assertEquals(TransactionStatus.APPROVED, event.transaction.status)
        assertEquals(CurrencyCode.parse("BRL"), event.transaction.currency)
        assertEquals(EventInstant.transactionTimestamp(DEFAULT_TIMESTAMP_MICROS), event.transaction.timestamp)
        assertEquals(EventInstant.accountCreatedAt(DEFAULT_ACCOUNT_CREATED_AT_MICROS), event.account.createdAt)
    }

    @Test
    fun `every ApplyResult outcome is covered by an exhaustive when and a duplicate remembers whether it conflicts`() {
        val outcomes: List<ApplyResult> = listOf(ApplyResult.Applied, ApplyResult.Obsolete, ApplyResult.Duplicate(conflicting = false))

        val labels =
            outcomes.map { outcome ->
                when (outcome) {
                    is ApplyResult.Applied -> "applied"
                    is ApplyResult.Obsolete -> "obsolete"
                    is ApplyResult.Duplicate -> "duplicate"
                }
            }

        assertEquals(listOf("applied", "obsolete", "duplicate"), labels)
        assertEquals(ApplyResult.Duplicate(false), outcomes[2])
        assertFalse((outcomes[2] as ApplyResult.Duplicate).conflicting)
        assertTrue(ApplyResult.Duplicate(conflicting = true).conflicting)
    }

    @Test
    fun `toString of the model never exposes balances, amounts or owners`() {
        val event = transactionEvent(transactionAmount = "97.07", balanceAmount = "183.12")
        val snapshot = BalanceSnapshot.from(event)
        val texts = listOf(event.transaction.toString(), event.account.toString(), snapshot.toString(), event.toString())

        texts.forEach { text ->
            assertFalse("97.07" in text, text)
            assertFalse("183.12" in text, text)
            assertFalse(DEFAULT_OWNER_ID in text, text)
        }
    }
}
