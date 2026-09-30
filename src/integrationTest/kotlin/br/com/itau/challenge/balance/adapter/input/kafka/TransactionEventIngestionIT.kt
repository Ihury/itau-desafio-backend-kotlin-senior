package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.support.EventPayloads
import br.com.itau.challenge.balance.support.SharedContextKafkaITBase
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertEquals

/**
 * Ingestao ponta a ponta com Redpanda e DynamoDB Local reais (`make integration-test`): publica no topico exclusivo, o
 * listener real consome, o servico grava e a consulta HTTP real reflete o saldo em ate 5 s apos a publicacao.
 * Cada teste usa contas aleatorias.
 */
class TransactionEventIngestionIT : SharedContextKafkaITBase() {
    @Test
    fun `a new account is created by the first event and reflected by the query`() {
        val account = newAccount()
        assertEquals(404, get(account).statusCode())

        publish(EventPayloads.transaction(account, balanceAmount = "183.12"))

        awaitBalance(account, "183.12", updatedAt = "2025-07-05T18:04:13.433-03:00")
    }

    @Test
    fun `a newer event replaces balance, owner and instant`() {
        val account = newAccount()
        publish(EventPayloads.transaction(account, timestampMicros = 1751749453433000L, balanceAmount = "100.00"))
        awaitBalance(account, "100.00")

        val newOwner = UUID.randomUUID().toString()
        publish(EventPayloads.transaction(account, timestampMicros = 1751749460000000L, balanceAmount = "250.50", ownerId = newOwner))

        awaitBalance(account, "250.50", owner = newOwner, updatedAt = "2025-07-05T18:04:20-03:00")
    }

    @Test
    fun `an older event does not replace the snapshot`() {
        val account = newAccount()
        publish(EventPayloads.transaction(account, timestampMicros = 1751749460000000L, balanceAmount = "250.50"))
        awaitBalance(account, "250.50")

        publish(EventPayloads.transaction(account, timestampMicros = 1751749453433000L, balanceAmount = "1.00"))
        val marker = newAccount()
        publish(EventPayloads.transaction(marker, balanceAmount = "5.00"))
        awaitBalance(marker, "5.00") // o marcador processado depois do evento antigo prova que ele ja foi consumido

        awaitBalance(account, "250.50")
    }

    @Test
    fun `events of interleaved accounts do not interfere with one another`() {
        val first = newAccount()
        val second = newAccount()

        publish(EventPayloads.transaction(first, balanceAmount = "10.00"))
        publish(EventPayloads.transaction(second, balanceAmount = "20.00"))
        publish(EventPayloads.transaction(first, timestampMicros = 1751749454000000L, balanceAmount = "11.00"))
        publish(EventPayloads.transaction(second, timestampMicros = 1751749455000000L, balanceAmount = "21.00"))

        awaitBalance(first, "11.00")
        awaitBalance(second, "21.00")
    }

    @Test
    fun `microseconds of the event are kept in updated_at`() {
        val account = newAccount()

        publish(EventPayloads.transaction(account, timestampMicros = 1751749453433123L))

        awaitBalance(account, "183.12", updatedAt = "2025-07-05T18:04:13.433123-03:00")
    }

    @Test
    fun `a newer declined event updates the snapshot like any other`() {
        val account = newAccount()
        publish(EventPayloads.transaction(account, timestampMicros = 1751749453433000L, balanceAmount = "100.00"))
        awaitBalance(account, "100.00")

        publish(EventPayloads.transaction(account, timestampMicros = 1751749453433001L, balanceAmount = "77.00", transactionStatus = "DECLINED"))

        awaitBalance(account, "77.00", updatedAt = "2025-07-05T18:04:13.433001-03:00")
    }

    @Test
    fun `an event that disables the account makes the query answer conflict`() {
        val account = newAccount()
        publish(EventPayloads.transaction(account, timestampMicros = 1751749453433000L, balanceAmount = "100.00"))
        awaitBalance(account, "100.00")

        publish(EventPayloads.transaction(account, timestampMicros = 1751749453433001L, accountStatus = "DISABLED"))

        await.atMost(SLO).untilAsserted { assertEquals(409, get(account).statusCode()) }
    }
}
