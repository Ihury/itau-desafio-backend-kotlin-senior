package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.support.EventPayloads
import br.com.itau.challenge.balance.support.IntegrationInfra
import br.com.itau.challenge.balance.support.KafkaIngestionITBase
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

/**
 * Convergencia sob desordem, duplicidade e reentrega pelo caminho real (Kafka -> listener -> DynamoDB -> consulta HTTP),
 * espelhando `quickstart.md` 5.1 a 5.4. Os testes que contam desfechos exatos publicam COM chave (a conta), para que a ordem de
 * chegada seja a de publicacao (mesma particao); o autorizador real publica sem chave e o servico converge em qualquer ordem.
 * Os desfechos sao medidos por delta do `MeterRegistry` (`balance.events{outcome}`).
 */
class ConvergenceIngestionIT : KafkaIngestionITBase() {
    private data class Outcomes(
        val processed: Int,
        val obsolete: Int,
        val duplicate: Int,
    ) {
        val total: Int get() = processed + obsolete + duplicate

        operator fun minus(other: Outcomes) = Outcomes(processed - other.processed, obsolete - other.obsolete, duplicate - other.duplicate)
    }

    private fun outcomes(): Outcomes {
        fun count(outcome: String) = meterRegistry.get("balance.events").tags("outcome", outcome, "reason", "none").counter().count().toInt()
        return Outcomes(count("processed"), count("obsolete"), count("duplicate"))
    }

    private fun tx(n: Int): String = "00000000-0000-4000-8000-%012d".format(n)

    /** Instante `n` segundos apos o base, com microssegundos (`...433123`) como no quickstart. */
    private fun t(n: Int): Long = 1751749453433123L + n * 1_000_000L

    private fun updatedAt(n: Int): String = "2025-07-05T18:04:%02d.433123-03:00".format(13 + n)

    @Suppress("LongParameterList")
    private fun send(
        account: String,
        txNumber: Int,
        instant: Int,
        amount: String,
        accountStatus: String = "ENABLED",
        transactionStatus: String = "APPROVED",
    ) = IntegrationInfra.publishKeyed(
        account,
        EventPayloads.transaction(
            account,
            timestampMicros = t(instant),
            transactionId = tx(txNumber),
            balanceAmount = amount,
            accountStatus = accountStatus,
            transactionStatus = transactionStatus,
        ),
    )

    private fun awaitTotal(
        baseline: Outcomes,
        events: Int,
    ) = await.atMost(SLO).untilAsserted { assertEquals(events, (outcomes() - baseline).total, "desfechos contabilizados") }

    private fun amountText(account: String): String {
        val response = get(account)
        assertEquals(200, response.statusCode())
        return json.readTree(response.body())["balance"]["amount"].decimalValue().toPlainString()
    }

    @Test
    fun `out of order events plus a duplicate converge to the highest instant and are counted exactly (5-1)`() {
        val account = newAccount()
        val baseline = outcomes()

        send(account, txNumber = 3, instant = 3, amount = "300.00")
        send(account, txNumber = 1, instant = 1, amount = "100.00")
        send(account, txNumber = 2, instant = 2, amount = "200.00")
        send(account, txNumber = 3, instant = 3, amount = "300.00")

        awaitTotal(baseline, 4)
        assertEquals(Outcomes(processed = 1, obsolete = 2, duplicate = 1), outcomes() - baseline)
        awaitBalance(account, "300.00", updatedAt = updatedAt(3))
        assertEquals("300.00", amountText(account))
    }

    @Test
    fun `redelivering the same messages changes nothing and only adds obsolete and duplicate outcomes (US3-6)`() {
        val account = newAccount()
        val baseline = outcomes()
        val messages = { send(account, 3, 3, "300.00"); send(account, 1, 1, "100.00"); send(account, 2, 2, "200.00"); send(account, 3, 3, "300.00") }
        messages()
        awaitTotal(baseline, 4)
        awaitBalance(account, "300.00", updatedAt = updatedAt(3))

        messages()

        awaitTotal(baseline, 8)
        // 2a rodada: tx3 duplicado (x2), tx1 e tx2 obsoletos; nada foi aplicado de novo
        assertEquals(Outcomes(processed = 1, obsolete = 4, duplicate = 3), outcomes() - baseline)
        awaitBalance(account, "300.00", updatedAt = updatedAt(3))
    }

    @Test
    fun `a timestamp tie is won by the greater transaction id in both arrival orders (5-2)`() {
        val account = newAccount()
        val reversedAccount = newAccount()
        val baseline = outcomes()

        send(account, txNumber = 11, instant = 5, amount = "20.00")
        send(account, txNumber = 10, instant = 5, amount = "10.00")
        send(reversedAccount, txNumber = 10, instant = 5, amount = "10.00")
        send(reversedAccount, txNumber = 11, instant = 5, amount = "20.00")

        awaitTotal(baseline, 4)
        awaitBalance(account, "20.00", updatedAt = updatedAt(5))
        awaitBalance(reversedAccount, "20.00", updatedAt = updatedAt(5))
        // ordem 11 -> 10: processed + obsolete; ordem 10 -> 11: processed + processed
        assertEquals(Outcomes(processed = 3, obsolete = 1, duplicate = 0), outcomes() - baseline)
    }

    @Test
    fun `declined events update, precision is exact and the response completes the scale of the currency (5-3)`() {
        val account = newAccount()
        val exact = "12345678901234567890.123456789012345678"

        send(account, txNumber = 20, instant = 1, amount = exact)
        awaitBalance(account, exact, updatedAt = updatedAt(1))
        assertEquals(exact, amountText(account))

        send(account, txNumber = 21, instant = 2, amount = "0.10", transactionStatus = "DECLINED")
        awaitBalance(account, "0.10", updatedAt = updatedAt(2))
        assertEquals("0.10", amountText(account))

        send(account, txNumber = 22, instant = 3, amount = "100")
        awaitBalance(account, "100.00", updatedAt = updatedAt(3))
        assertEquals("100.00", amountText(account))

        send(account, txNumber = 23, instant = 4, amount = "10.123")
        awaitBalance(account, "10.123", updatedAt = updatedAt(4))
        assertEquals("10.123", amountText(account))
    }

    @Test
    fun `the disabled cycle answers 200 then 409, ignores an older event and answers 200 again (5-4)`() {
        val account = newAccount()

        send(account, txNumber = 30, instant = 1, amount = "50.00")
        awaitBalance(account, "50.00")

        send(account, txNumber = 31, instant = 2, amount = "50.00", accountStatus = "DISABLED")
        await.atMost(SLO).untilAsserted { assertEquals(409, get(account).statusCode()) }

        val baseline = outcomes()
        send(account, txNumber = 29, instant = 0, amount = "40.00")
        awaitTotal(baseline, 1)
        assertEquals(Outcomes(processed = 0, obsolete = 1, duplicate = 0), outcomes() - baseline)
        assertEquals(409, get(account).statusCode(), "o evento antigo nao reabilita a conta")

        send(account, txNumber = 32, instant = 3, amount = "70.00")
        awaitBalance(account, "70.00", updatedAt = updatedAt(3))
        assertEquals("70.00", amountText(account))
    }
}
