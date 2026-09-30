package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.support.EventPayloads
import br.com.itau.challenge.balance.support.IntegrationInfra
import br.com.itau.challenge.balance.support.SharedContextKafkaITBase
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

/**
 * Convergencia sob desordem, duplicidade e reentrega pelo caminho real (Kafka -> listener -> DynamoDB -> consulta HTTP).
 * Os testes que contam desfechos exatos publicam COM chave (a conta), para que a ordem de chegada seja a de publicacao
 * (mesma particao); o autorizador real publica sem chave e o servico converge em qualquer ordem.
 * Os desfechos sao medidos por delta do `MeterRegistry` (`balance.events{outcome}`).
 */
class ConvergenceIngestionIT : SharedContextKafkaITBase() {
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

    /** Baseline dos desfechos depois de uma barreira de lag zero: nada em voo pode ser contado depois da leitura. */
    private fun baselineOutcomes(): Outcomes {
        topics.awaitGroupLagZero()
        return outcomes()
    }

    private fun transactionId(n: Int): String = "00000000-0000-4000-8000-%012d".format(n)

    /** Instante `n` segundos apos o base, com microssegundos (`...433123`). */
    private fun instantMicros(secondsAfterBase: Int): Long = 1751749453433123L + secondsAfterBase * 1_000_000L

    private fun expectedUpdatedAt(secondsAfterBase: Int): String = "2025-07-05T18:04:%02d.433123-03:00".format(13 + secondsAfterBase)

    @Suppress("LongParameterList")
    private fun publishEvent(
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
            timestampMicros = instantMicros(instant),
            transactionId = transactionId(txNumber),
            balanceAmount = amount,
            accountStatus = accountStatus,
            transactionStatus = transactionStatus,
        ),
    )

    private fun awaitOutcomeTotal(
        baseline: Outcomes,
        events: Int,
    ) = await.atMost(SLO).untilAsserted { assertEquals(events, (outcomes() - baseline).total, "desfechos contabilizados") }

    private fun queriedAmount(account: String): String {
        val response = get(account)
        assertEquals(200, response.statusCode())
        return json.readTree(response.body())["balance"]["amount"].decimalValue().toPlainString()
    }

    @Test
    fun `out of order events plus a duplicate converge to the highest instant and are counted exactly`() {
        val account = newAccount()
        val baseline = baselineOutcomes()

        publishEvent(account, txNumber = 3, instant = 3, amount = "300.00")
        publishEvent(account, txNumber = 1, instant = 1, amount = "100.00")
        publishEvent(account, txNumber = 2, instant = 2, amount = "200.00")
        publishEvent(account, txNumber = 3, instant = 3, amount = "300.00")

        awaitOutcomeTotal(baseline, 4)
        assertEquals(Outcomes(processed = 1, obsolete = 2, duplicate = 1), outcomes() - baseline)
        awaitBalance(account, "300.00", updatedAt = expectedUpdatedAt(3))
        assertEquals("300.00", queriedAmount(account))
    }

    @Test
    fun `redelivering the same messages changes nothing and only adds obsolete and duplicate outcomes`() {
        val account = newAccount()
        val baseline = baselineOutcomes()
        val publishRound = {
            publishEvent(account, 3, 3, "300.00")
            publishEvent(account, 1, 1, "100.00")
            publishEvent(account, 2, 2, "200.00")
            publishEvent(account, 3, 3, "300.00")
        }
        publishRound()
        awaitOutcomeTotal(baseline, 4)
        awaitBalance(account, "300.00", updatedAt = expectedUpdatedAt(3))

        publishRound()

        awaitOutcomeTotal(baseline, 8)
        // 2a rodada: tx3 duplicado (x2), tx1 e tx2 obsoletos; nada foi aplicado de novo
        assertEquals(Outcomes(processed = 1, obsolete = 4, duplicate = 3), outcomes() - baseline)
        awaitBalance(account, "300.00", updatedAt = expectedUpdatedAt(3))
    }

    @Test
    fun `a timestamp tie is won by the greater transaction id in both arrival orders`() {
        val account = newAccount()
        val reversedAccount = newAccount()
        val baseline = baselineOutcomes()

        publishEvent(account, txNumber = 11, instant = 5, amount = "20.00")
        publishEvent(account, txNumber = 10, instant = 5, amount = "10.00")
        publishEvent(reversedAccount, txNumber = 10, instant = 5, amount = "10.00")
        publishEvent(reversedAccount, txNumber = 11, instant = 5, amount = "20.00")

        awaitOutcomeTotal(baseline, 4)
        awaitBalance(account, "20.00", updatedAt = expectedUpdatedAt(5))
        awaitBalance(reversedAccount, "20.00", updatedAt = expectedUpdatedAt(5))
        // ordem 11 -> 10: processed + obsolete; ordem 10 -> 11: processed + processed
        assertEquals(Outcomes(processed = 3, obsolete = 1, duplicate = 0), outcomes() - baseline)
    }

    @Test
    fun `declined events update, precision is exact and the response completes the scale of the currency`() {
        val account = newAccount()
        val exact = "12345678901234567890.123456789012345678"

        publishEvent(account, txNumber = 20, instant = 1, amount = exact)
        awaitBalance(account, exact, updatedAt = expectedUpdatedAt(1))
        assertEquals(exact, queriedAmount(account))

        publishEvent(account, txNumber = 21, instant = 2, amount = "0.10", transactionStatus = "DECLINED")
        awaitBalance(account, "0.10", updatedAt = expectedUpdatedAt(2))
        assertEquals("0.10", queriedAmount(account))

        publishEvent(account, txNumber = 22, instant = 3, amount = "100")
        awaitBalance(account, "100.00", updatedAt = expectedUpdatedAt(3))
        assertEquals("100.00", queriedAmount(account))

        publishEvent(account, txNumber = 23, instant = 4, amount = "10.123")
        awaitBalance(account, "10.123", updatedAt = expectedUpdatedAt(4))
        assertEquals("10.123", queriedAmount(account))
    }

    @Test
    fun `the disabled cycle answers 200 then 409, ignores an older event and answers 200 again`() {
        val account = newAccount()

        publishEvent(account, txNumber = 30, instant = 1, amount = "50.00")
        awaitBalance(account, "50.00")

        publishEvent(account, txNumber = 31, instant = 2, amount = "50.00", accountStatus = "DISABLED")
        await.atMost(SLO).untilAsserted { assertEquals(409, get(account).statusCode()) }

        val baseline = baselineOutcomes()
        publishEvent(account, txNumber = 29, instant = 0, amount = "40.00")
        awaitOutcomeTotal(baseline, 1)
        assertEquals(Outcomes(processed = 0, obsolete = 1, duplicate = 0), outcomes() - baseline)
        assertEquals(409, get(account).statusCode(), "o evento antigo nao reabilita a conta")

        publishEvent(account, txNumber = 32, instant = 3, amount = "70.00")
        awaitBalance(account, "70.00", updatedAt = expectedUpdatedAt(3))
        assertEquals("70.00", queriedAmount(account))
    }
}
