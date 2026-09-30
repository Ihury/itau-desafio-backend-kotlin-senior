package br.com.itau.challenge.balance.adapter.input.web.dto

import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.TransactionEventFixtures.transactionEvent
import org.junit.jupiter.api.Test
import tools.jackson.core.StreamWriteFeature
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.jacksonMapperBuilder
import java.time.ZoneId
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class BalanceResponseTest {
    /** Mesma configuracao do `application.yaml`: `spring.jackson.write.write-bigdecimal-as-plain=true`. */
    private val mapper: JsonMapper = jacksonMapperBuilder().enable(StreamWriteFeature.WRITE_BIGDECIMAL_AS_PLAIN).build()

    private val saoPaulo = ZoneId.of("America/Sao_Paulo")

    private fun responseJson(
        balanceAmount: String = "183.12",
        balanceCurrency: String = "BRL",
        timestampMicros: Long = 1751749453433000L,
        zone: ZoneId = saoPaulo,
    ): String =
        mapper.writeValueAsString(
            BalanceResponse.from(
                BalanceSnapshot.from(
                    transactionEvent(balanceAmount = balanceAmount, balanceCurrency = balanceCurrency, timestampMicros = timestampMicros),
                ),
                zone,
            ),
        )

    private fun amountOf(
        balanceAmount: String,
        balanceCurrency: String = "BRL",
    ): String = Regex("\"amount\":([^,}]+)").find(responseJson(balanceAmount, balanceCurrency))!!.groupValues[1]

    @Test
    fun `serializes exactly the contract of the client example`() {
        assertEquals(
            """{"id":"5b19c8b6-0cc4-4c72-a989-0c2ee15fa975","owner":"315e3cfe-f4af-4cd2-b298-a449e614349a",""" +
                """"balance":{"amount":183.12,"currency":"BRL"},"updated_at":"2025-07-05T18:04:13.433-03:00"}""",
            responseJson(),
        )
    }

    @Test
    fun `amount completes the currency fraction digits without rounding and never uses scientific notation`() {
        assertEquals("183.10", amountOf("183.1"))
        assertEquals("100.00", amountOf("100"))
        assertEquals("10.123", amountOf("10.123"))
        assertEquals("500", amountOf("500", "JPY"))
        assertEquals("500.5", amountOf("500.5", "JPY"))
        assertEquals("1000.00", amountOf("1E+3"))
        assertEquals("0.0000001", amountOf("0.0000001"))
        assertEquals("0.0000001", amountOf("1E-7"))
        assertEquals("0.00", amountOf("0"))
        assertEquals("-5.00", amountOf("-5"))
        assertEquals("12345678901234567890.123456789012345678", amountOf("12345678901234567890.123456789012345678"))
    }

    @Test
    fun `updated at preserves microseconds and omits the fraction when the second is whole`() {
        assertEquals("2025-07-05T18:04:13.433123-03:00", updatedAt(1751749453433123L))
        assertEquals("2025-07-05T18:04:13.433-03:00", updatedAt(1751749453433000L))
        assertEquals("2025-07-05T18:04:13-03:00", updatedAt(1751749453000000L))
        assertEquals("2025-07-05T18:04:13.000001-03:00", updatedAt(1751749453000001L))
    }

    @Test
    fun `updated at uses the zone rules and not a fixed offset`() {
        // 2018-01-15T12:00:00Z: horario de verao no Brasil (-02:00); 2025-07-05: sem horario de verao (-03:00)
        assertEquals("2018-01-15T10:00:00-02:00", updatedAt(1516017600000000L))
        assertEquals("2025-07-05T21:04:13.433Z", updatedAt(1751749453433000L, ZoneId.of("UTC")))
    }

    @Test
    fun `ids are lowercase and the field is named updated_at`() {
        val text = mapper.writeValueAsString(BalanceResponse.from(BalanceSnapshot.from(transactionEvent(accountId = "5B19C8B6-0CC4-4C72-A989-0C2EE15FA975")), saoPaulo))

        assertTrue(text.contains("\"id\":\"5b19c8b6-0cc4-4c72-a989-0c2ee15fa975\""))
        assertTrue(text.contains("\"updated_at\":"))
        assertFalse(text.contains("updatedAt"))
    }

    @Test
    fun `currency of the balance is exposed as is without conversion`() {
        assertTrue(responseJson(balanceCurrency = "USD").contains("\"currency\":\"USD\""))
    }

    private fun updatedAt(
        micros: Long,
        zone: ZoneId = saoPaulo,
    ): String = Regex("\"updated_at\":\"([^\"]+)\"").find(responseJson(timestampMicros = micros, zone = zone))!!.groupValues[1]
}
