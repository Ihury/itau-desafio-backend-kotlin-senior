package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.AccountStatus
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.TransactionEventFixtures.DEFAULT_ACCOUNT_ID
import br.com.itau.challenge.balance.domain.model.TransactionEventFixtures.transactionEvent
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class BalanceItemMapperTest {
    private fun snapshot(
        balanceAmount: String = "183.12",
        balanceCurrency: String = "BRL",
        accountStatus: AccountStatus = AccountStatus.ENABLED,
        timestampMicros: Long = 1751749453433000L,
        accountId: String = DEFAULT_ACCOUNT_ID,
    ): BalanceSnapshot =
        BalanceSnapshot.from(
            transactionEvent(
                balanceAmount = balanceAmount,
                balanceCurrency = balanceCurrency,
                accountStatus = accountStatus,
                timestampMicros = timestampMicros,
                accountId = accountId,
            ),
        )

    private fun validItem(): MutableMap<String, AttributeValue> = BalanceItemMapper.toItem(snapshot()).toMutableMap()

    private fun s(value: String): AttributeValue = AttributeValue.builder().s(value).build()

    private fun n(value: String): AttributeValue = AttributeValue.builder().n(value).build()

    @Test
    fun `toItem produces exactly the attributes of the data model`() {
        val item = BalanceItemMapper.toItem(snapshot())

        assertEquals(
            mapOf(
                "pk" to "S:ACCOUNT#5b19c8b6-0cc4-4c72-a989-0c2ee15fa975",
                "sk" to "S:BALANCE",
                "schemaVersion" to "N:1",
                "ownerId" to "S:315e3cfe-f4af-4cd2-b298-a449e614349a",
                "accountStatus" to "S:ENABLED",
                "balanceAmount" to "N:183.12",
                "balanceCurrency" to "S:BRL",
                "accountCreatedAtMicros" to "N:1634874339000000",
                "lastTxTsMicros" to "N:1751749453433000",
                "lastTxId" to "S:8e8ae808-b154-48b5-9f3e-553935cc4543",
            ),
            item.mapValues { (_, v) -> if (v.s() != null) "S:${v.s()}" else "N:${v.n()}" },
        )
    }

    @Test
    fun `key is the partition and sort key of the account in lowercase`() {
        val key = BalanceItemMapper.keyOf(AccountId.parse(DEFAULT_ACCOUNT_ID.uppercase()))

        assertEquals(setOf("pk", "sk"), key.keys)
        assertEquals("ACCOUNT#$DEFAULT_ACCOUNT_ID", key.getValue("pk").s())
        assertEquals("BALANCE", key.getValue("sk").s())
    }

    @Test
    fun `balance amount is written as plain number without scientific notation`() {
        assertEquals("0.0000001", BalanceItemMapper.toItem(snapshot(balanceAmount = "1E-7")).getValue("balanceAmount").n())
        assertEquals("1000", BalanceItemMapper.toItem(snapshot(balanceAmount = "1E+3")).getValue("balanceAmount").n())
        val thirtyEight = "12345678901234567890.123456789012345678"
        assertEquals(thirtyEight, BalanceItemMapper.toItem(snapshot(balanceAmount = thirtyEight)).getValue("balanceAmount").n())
    }

    @Test
    fun `round trip of the stored number keeps the exact value even when the database normalizes the scale`() {
        val item = validItem().apply { put("balanceAmount", n("183.1")) }

        val restored = BalanceItemMapper.fromItem(item)

        assertEquals(snapshot(balanceAmount = "183.10").balance, restored.balance)
        assertEquals(BigDecimal("183.10"), restored.balance.paddedToCurrencyScale())
        assertEquals(0, BigDecimal("183.1").compareTo(restored.balance.amount))
    }

    @Test
    fun `round trip keeps negative values, 38 digits and microseconds`() {
        val negative = snapshot(balanceAmount = "-5.5", timestampMicros = 1751749453433123L)
        assertEquals(negative, BalanceItemMapper.fromItem(BalanceItemMapper.toItem(negative)))

        val thirtyEight = snapshot(balanceAmount = "12345678901234567890.123456789012345678")
        val restored = BalanceItemMapper.fromItem(BalanceItemMapper.toItem(thirtyEight))
        assertEquals(BigDecimal("12345678901234567890.123456789012345678"), restored.balance.amount)

        assertEquals(1751749453433123L, BalanceItemMapper.fromItem(BalanceItemMapper.toItem(negative)).precedence.timestamp.micros)
    }

    @Test
    fun `a persisted snapshot is trusted and read back even with timestamps below the default plausibility minimums`() {
        // os minimos (BALANCE_MIN_*) sao configuraveis e so se aplicam na ESCRITA; a leitura nao os reaplica (nem vira 500)
        val transaction1995 = 803_174_400_000_000L
        val created1850 = -3_155_760_000_000_000L
        val item =
            validItem().apply {
                put("lastTxTsMicros", n(transaction1995.toString()))
                put("accountCreatedAtMicros", n(created1850.toString()))
            }

        val restored = BalanceItemMapper.fromItem(item)

        assertEquals(transaction1995, restored.precedence.timestamp.micros)
        assertEquals(created1850, restored.accountCreatedAt.micros)
    }

    @Test
    fun `fromItem rebuilds every field of the snapshot`() {
        val original = snapshot(accountStatus = AccountStatus.DISABLED, balanceCurrency = "USD")

        val restored = BalanceItemMapper.fromItem(BalanceItemMapper.toItem(original))

        assertEquals(original, restored)
        assertEquals(AccountStatus.DISABLED, restored.status)
        assertEquals("USD", restored.balance.currency.value)
    }

    @Test
    fun `missing attribute is a corrupted item and never a wrong value`() {
        listOf(
            "pk", "sk", "schemaVersion", "ownerId", "accountStatus", "balanceAmount",
            "balanceCurrency", "accountCreatedAtMicros", "lastTxTsMicros", "lastTxId",
        ).forEach { attribute ->
            val item = validItem().apply { remove(attribute) }

            val failure = assertFailsWith<IllegalStateException>(attribute) { BalanceItemMapper.fromItem(item) }
            assertTrue(attribute in failure.message.orEmpty(), "a mensagem aponta o atributo: ${failure.message}")
        }
    }

    @Test
    fun `wrongly typed attribute is a corrupted item`() {
        assertFailsWith<IllegalStateException> { BalanceItemMapper.fromItem(validItem().apply { put("balanceAmount", s("183.12")) }) }
        assertFailsWith<IllegalStateException> { BalanceItemMapper.fromItem(validItem().apply { put("ownerId", n("1")) }) }
    }

    @Test
    fun `invalid numbers are a corrupted item and the message never carries the offending text`() {
        val failure =
            assertFailsWith<IllegalStateException> {
                BalanceItemMapper.fromItem(validItem().apply { put("balanceAmount", n("12abc34")) })
            }
        assertFalse("12abc34" in failure.message.orEmpty())
        assertNull(failure.cause, "causas de parser podem conter o valor")

        assertFailsWith<IllegalStateException> { BalanceItemMapper.fromItem(validItem().apply { put("lastTxTsMicros", n("1.5")) }) }
    }

    @Test
    fun `unknown domain values and out of bounds data become IllegalStateException and never InvalidEventException`() {
        val cases =
            listOf(
                validItem().apply { put("accountStatus", s("SUSPENDED")) },
                validItem().apply { put("balanceCurrency", s("brl")) },
                validItem().apply { put("ownerId", s("1-1-1-1-1")) },
                validItem().apply { put("lastTxId", s("not-a-uuid")) },
                validItem().apply { put("lastTxTsMicros", n("not-a-number")) },
                validItem().apply { put("accountCreatedAtMicros", n("1.5")) },
                validItem().apply { put("balanceAmount", n("123456789012345678901234567890123456789")) },
                validItem().apply { put("pk", s("ACCOUNT#xyz")) },
            )

        cases.forEach { item ->
            val failure = runCatching { BalanceItemMapper.fromItem(item) }.exceptionOrNull()
            assertTrue(failure is IllegalStateException, "esperado IllegalStateException, veio $failure")
            assertFalse(InvalidEventException::class.java.isInstance(failure))
        }
    }

    @Test
    fun `unexpected key layout or schema version is a corrupted item`() {
        assertFailsWith<IllegalStateException> { BalanceItemMapper.fromItem(validItem().apply { put("pk", s("OWNER#$DEFAULT_ACCOUNT_ID")) }) }
        assertFailsWith<IllegalStateException> { BalanceItemMapper.fromItem(validItem().apply { put("sk", s("TX#1")) }) }
        assertFailsWith<IllegalStateException> { BalanceItemMapper.fromItem(validItem().apply { put("schemaVersion", n("2")) }) }
    }
}
