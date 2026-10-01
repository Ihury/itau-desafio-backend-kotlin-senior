package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertTimeoutPreemptively
import java.math.BigDecimal
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals

class MoneyTest {
    private val brl = CurrencyCode.parse("BRL")

    private fun money(
        amount: String,
        currency: CurrencyCode = brl,
    ) = Money.of(BigDecimal(amount), currency)

    private fun assertInvalidValue(amount: String) {
        val error = assertFailsWith<InvalidEventException>("aceitou $amount") { money(amount) }
        assertEquals(RejectionReason.INVALID_VALUE, error.reason, "com $amount")
    }

    @Test
    fun `accepts 38 significant digits`() {
        val amount = "12345678901234567890.123456789012345678"
        assertEquals(BigDecimal(amount), money(amount).amount)
    }

    @Test
    fun `rejects 39 significant digits`() {
        assertInvalidValue("12345678901234567890.1234567890123456789")
        assertInvalidValue("1".repeat(39))
    }

    @Test
    fun `accepts scale 38 and rejects scale 39`() {
        assertEquals(BigDecimal("0." + "0".repeat(37) + "1"), money("0." + "0".repeat(37) + "1").amount)
        assertInvalidValue("0." + "0".repeat(38) + "1")
    }

    @Test
    fun `expands a negative scale to scale zero when the digits fit in 38`() {
        assertEquals("1000", money("1E+3").amount.toPlainString())
        assertEquals(0, money("1E+3").amount.scale())
        assertEquals("1500", money("1.5E+3").amount.toPlainString())
        assertEquals(38, money("1E+37").amount.precision())
    }

    @Test
    fun `rejects a negative scale whose expansion exceeds 38 digits without expanding it`() {
        assertInvalidValue("1E+38")
        assertTimeoutPreemptively(Duration.ofSeconds(2)) { assertInvalidValue("1E999999999") }
        assertTimeoutPreemptively(Duration.ofSeconds(2)) { assertInvalidValue("1E+2147483647") }
    }

    @Test
    fun `precision is measured on the received value, so one followed by 38 zeros has 39 digits and is rejected`() {
        assertInvalidValue("1." + "0".repeat(38))
    }

    @Test
    fun `precision is measured on the received value, so one followed by 37 zeros has 38 digits and is accepted`() {
        assertEquals(38, money("1." + "0".repeat(37)).amount.precision())
    }

    @Test
    fun `zero and negative amounts are valid`() {
        assertEquals(0, money("0").amount.signum())
        assertEquals(0, money("0.00").amount.signum())
        assertEquals(-1, money("-5").amount.signum())
        assertEquals(BigDecimal("-0.01"), money("-0.01").amount)
    }

    @Test
    fun `equality and hashCode are by numeric value`() {
        assertEquals(money("183.10"), money("183.1"))
        assertEquals(money("183.10").hashCode(), money("183.1").hashCode())
        assertEquals(money("0.00"), money("0"))
        assertEquals(money("0.00").hashCode(), money("0").hashCode())
        assertEquals(money("1E+3"), money("1000"))
        assertNotEquals(money("183.10"), money("183.11"))
        assertNotEquals(money("183.10"), money("183.10", CurrencyCode.parse("USD")))
        assertNotEquals<Any>(money("183.10"), "183.10")
    }

    @Test
    fun `toString never exposes the amount`() {
        val text = money("183.12").toString()
        assertEquals("Money(BRL)", text)
    }

    @Test
    fun `presentation completes the scale to the currency fraction digits`() {
        assertEquals("183.10", money("183.1").paddedToCurrencyScale().toPlainString())
        assertEquals("100.00", money("100").paddedToCurrencyScale().toPlainString())
        assertEquals("10.123", money("10.123").paddedToCurrencyScale().toPlainString())
        assertEquals("0.00", money("0").paddedToCurrencyScale().toPlainString())
        assertEquals("-5.00", money("-5").paddedToCurrencyScale().toPlainString())
        assertEquals("1000.00", money("1E+3").paddedToCurrencyScale().toPlainString())
    }

    @Test
    fun `presentation keeps currencies with zero fraction digits and never rounds`() {
        val jpy = CurrencyCode.parse("JPY")
        assertEquals("500", money("500", jpy).paddedToCurrencyScale().toPlainString())
        assertEquals("500.5", money("500.5", jpy).paddedToCurrencyScale().toPlainString())
    }

    @Test
    fun `presentation leaves currencies without fraction digits unchanged`() {
        val xau = CurrencyCode.parse("XAU")
        assertEquals(-1, xau.currency.defaultFractionDigits)
        assertEquals("3", money("3", xau).paddedToCurrencyScale().toPlainString())
        assertEquals("3.5", money("3.5", xau).paddedToCurrencyScale().toPlainString())
    }

    @Test
    fun `presentation never changes the numeric value and never uses scientific notation`() {
        listOf("183.1", "100", "10.123", "0", "-5", "0.0000001", "1E+3", "12345678901234567890.123456789012345678").forEach {
            val original = money(it).amount
            val presented = money(it).paddedToCurrencyScale()
            assertEquals(0, original.compareTo(presented), "valor alterado para $it")
            assertFalse('E' in presented.toPlainString(), "notacao cientifica para $it")
        }
        assertEquals("0.0000001", money("0.0000001").paddedToCurrencyScale().toPlainString())
    }
}
