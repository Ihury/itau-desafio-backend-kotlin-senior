package br.com.itau.challenge.balance.testing

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.AccountStatus
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.TransactionEvent
import br.com.itau.challenge.balance.domain.model.TransactionEventFixtures.transactionEvent
import br.com.itau.challenge.balance.domain.model.TransactionStatus
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * Contrato do [BalanceSnapshotWriter]: a MESMA suite roda contra o fake em memoria (sem infraestrutura) e contra o
 * DynamoDB Local (integracao), provando que o fake so e confiavel porque obedece ao que o banco real faz
 * (data-model.md secao 7). Cada teste usa uma conta aleatoria, para poder rodar contra um banco compartilhado.
 *
 * Cobre criar, substituir, obsoleto, isolamento, microssegundos, saldo exato e status (US2) e, na convergencia (US3), a
 * classificacao de duplicado x obsoleto x anomalia (`conflicting_duplicate`) e o desempate por `transactionId`.
 */
abstract class BalanceSnapshotWriterContract {
    /** Escritor sob teste. */
    protected abstract val writer: BalanceSnapshotWriter

    /** Snapshot vigente da conta visto DIRETAMENTE no armazenamento (nao pelo escritor); `null` se ausente. */
    protected abstract fun currentOf(accountId: AccountId): BalanceSnapshot?

    private val baseTs = 1751749453433000L

    private fun newAccountId(): String = UUID.randomUUID().toString()

    private fun event(
        accountId: String,
        timestampMicros: Long = baseTs,
        transactionId: String = UUID.randomUUID().toString(),
        balanceAmount: String = "183.12",
        ownerId: String = "315e3cfe-f4af-4cd2-b298-a449e614349a",
        accountStatus: AccountStatus = AccountStatus.ENABLED,
        transactionStatus: TransactionStatus = TransactionStatus.APPROVED,
        balanceCurrency: String = "BRL",
    ): TransactionEvent =
        transactionEvent(
            accountId = accountId,
            timestampMicros = timestampMicros,
            transactionId = transactionId,
            balanceAmount = balanceAmount,
            ownerId = ownerId,
            accountStatus = accountStatus,
            transactionStatus = transactionStatus,
            balanceCurrency = balanceCurrency,
        )

    private fun apply(event: TransactionEvent): ApplyResult = writer.applyIfNewer(BalanceSnapshot.from(event))

    private fun stored(accountId: String): BalanceSnapshot = assertNotNull(currentOf(AccountId.parse(accountId)))

    @Test
    fun `an initial event creates the snapshot`() {
        val account = newAccountId()
        val initial = event(account)

        assertEquals(ApplyResult.Applied, apply(initial))

        assertEquals(BalanceSnapshot.from(initial), stored(account))
    }

    @Test
    fun `a newer event replaces every field coherently`() {
        val account = newAccountId()
        apply(event(account, timestampMicros = baseTs, balanceAmount = "100.00", ownerId = "aaaaaaaa-f4af-4cd2-b298-a449e614349a"))
        val newer = event(account, timestampMicros = baseTs + 1, balanceAmount = "250.50", ownerId = "bbbbbbbb-f4af-4cd2-b298-a449e614349a", balanceCurrency = "USD")

        assertEquals(ApplyResult.Applied, apply(newer))

        assertEquals(BalanceSnapshot.from(newer), stored(account))
    }

    @Test
    fun `an older event is obsolete and does not change the snapshot`() {
        val account = newAccountId()
        val current = event(account, timestampMicros = baseTs, balanceAmount = "100.00")
        apply(current)

        val result = apply(event(account, timestampMicros = baseTs - 1, balanceAmount = "1.00", ownerId = "cccccccc-f4af-4cd2-b298-a449e614349a"))

        assertEquals(ApplyResult.Obsolete, result)
        assertEquals(BalanceSnapshot.from(current), stored(account))
    }

    @Test
    fun `accounts are isolated from one another`() {
        val first = newAccountId()
        val second = newAccountId()
        apply(event(first, balanceAmount = "10.00"))
        apply(event(second, balanceAmount = "20.00"))

        apply(event(first, timestampMicros = baseTs + 5, balanceAmount = "11.00"))

        assertEquals(BigDecimal("11.00"), stored(first).balance.amount)
        assertEquals(BigDecimal("20.00"), stored(second).balance.amount)
    }

    @Test
    fun `an account that was never written has no snapshot`() {
        assertNull(currentOf(AccountId.parse(newAccountId())))
    }

    @Test
    fun `microseconds of the event are preserved`() {
        val account = newAccountId()

        apply(event(account, timestampMicros = 1751749453433123L))

        assertEquals(1751749453433123L, stored(account).precedence.timestamp.micros)
    }

    @Test
    fun `exact decimal balances read back numerically identical`() {
        listOf("12345678901234567890.123456789012345678", "0.10", "183.10", "-42.50", "0").forEach { amount ->
            val account = newAccountId()

            apply(event(account, balanceAmount = amount))

            assertEquals(0, BigDecimal(amount).compareTo(stored(account).balance.amount), "saldo $amount")
        }
    }

    @Test
    fun `declined and disabled events take part in the precedence like any other`() {
        val account = newAccountId()
        apply(event(account, timestampMicros = baseTs, balanceAmount = "100.00"))

        val declined = event(account, timestampMicros = baseTs + 1, transactionStatus = TransactionStatus.DECLINED, balanceAmount = "90.00")
        val disabled = event(account, timestampMicros = baseTs + 2, accountStatus = AccountStatus.DISABLED, balanceAmount = "80.00")

        assertEquals(ApplyResult.Applied, apply(declined))
        assertEquals(BigDecimal("90.00"), stored(account).balance.amount)
        assertEquals(ApplyResult.Applied, apply(disabled))
        assertEquals(AccountStatus.DISABLED, stored(account).status)
        assertEquals(ApplyResult.Obsolete, apply(event(account, timestampMicros = baseTs + 1, balanceAmount = "1.00", accountStatus = AccountStatus.ENABLED)))
        assertEquals(AccountStatus.DISABLED, stored(account).status)
    }

    // ---- US3: convergencia, duplicidade e desempate ----

    private val idLow = "00000000-0000-4000-8000-000000000001"
    private val idHigh = "ffffffff-ffff-4fff-8fff-ffffffffff01"

    @Test
    fun `the same event delivered again is a duplicate and nothing changes`() {
        val account = newAccountId()
        val original = event(account, balanceAmount = "100.00")
        apply(original)

        assertEquals(ApplyResult.Duplicate(conflicting = false), apply(original))

        assertEquals(BalanceSnapshot.from(original), stored(account))
    }

    @Test
    fun `the same key with divergent content is a conflicting duplicate and the first one prevails`() {
        val divergences: Map<String, (String, String) -> TransactionEvent> =
            mapOf(
                "dono" to { account, tx -> event(account, transactionId = tx, ownerId = "dddddddd-f4af-4cd2-b298-a449e614349a") },
                "situacao da conta" to { account, tx -> event(account, transactionId = tx, accountStatus = AccountStatus.DISABLED) },
                "moeda" to { account, tx -> event(account, transactionId = tx, balanceCurrency = "USD") },
                "saldo" to { account, tx -> event(account, transactionId = tx, balanceAmount = "999.99") },
            )
        divergences.forEach { (field, divergent) ->
            val account = newAccountId()
            val tx = UUID.randomUUID().toString()
            val first = event(account, transactionId = tx, balanceAmount = "100.00")
            apply(first)

            assertEquals(ApplyResult.Duplicate(conflicting = true), apply(divergent(account, tx)), "divergencia em $field")

            assertEquals(BalanceSnapshot.from(first), stored(account), "o primeiro prevalece apos divergencia em $field")
        }
    }

    @Test
    fun `a balance that differs only by scale is not a divergence`() {
        val account = newAccountId()
        val tx = UUID.randomUUID().toString()
        apply(event(account, transactionId = tx, balanceAmount = "100.00"))

        assertEquals(ApplyResult.Duplicate(conflicting = false), apply(event(account, transactionId = tx, balanceAmount = "100")))
        assertEquals(ApplyResult.Duplicate(conflicting = false), apply(event(account, transactionId = tx, balanceAmount = "100.000")))
    }

    @Test
    fun `a redelivery of a transaction that was already superseded is obsolete`() {
        val account = newAccountId()
        val old = event(account, timestampMicros = baseTs, balanceAmount = "100.00")
        val newer = event(account, timestampMicros = baseTs + 10, balanceAmount = "200.00")
        apply(old)
        apply(newer)

        assertEquals(ApplyResult.Obsolete, apply(old))

        assertEquals(BalanceSnapshot.from(newer), stored(account))
    }

    @Test
    fun `on a timestamp tie the greater transaction id wins in both arrival orders`() {
        val lowFirst = newAccountId()
        val low = { account: String -> event(account, transactionId = idLow, balanceAmount = "10.00") }
        val high = { account: String -> event(account, transactionId = idHigh, balanceAmount = "20.00") }

        assertEquals(ApplyResult.Applied, apply(low(lowFirst)))
        assertEquals(ApplyResult.Applied, apply(high(lowFirst)))
        assertEquals(BalanceSnapshot.from(high(lowFirst)), stored(lowFirst))

        val highFirst = newAccountId()
        assertEquals(ApplyResult.Applied, apply(high(highFirst)))
        assertEquals(ApplyResult.Obsolete, apply(low(highFirst)))
        assertEquals(BalanceSnapshot.from(high(highFirst)), stored(highFirst))
    }

    @Test
    fun `the transaction id is normalized to lower case before comparing`() {
        val account = newAccountId()
        val lower = "8e8ae808-b154-48b5-9f3e-553935cc4543"
        apply(event(account, transactionId = lower.uppercase(), balanceAmount = "100.00"))

        assertEquals(ApplyResult.Duplicate(conflicting = false), apply(event(account, transactionId = lower, balanceAmount = "100.00")))
        assertEquals("8e8ae808-b154-48b5-9f3e-553935cc4543", stored(account).precedence.transactionId.value)
    }

    @Test
    fun `the same transaction id in different accounts does not interfere`() {
        val first = newAccountId()
        val second = newAccountId()
        val tx = UUID.randomUUID().toString()

        assertEquals(ApplyResult.Applied, apply(event(first, transactionId = tx, balanceAmount = "10.00")))
        assertEquals(ApplyResult.Applied, apply(event(second, transactionId = tx, balanceAmount = "20.00")))

        assertEquals(BigDecimal("10.00"), stored(first).balance.amount)
        assertEquals(BigDecimal("20.00"), stored(second).balance.amount)
    }
}
