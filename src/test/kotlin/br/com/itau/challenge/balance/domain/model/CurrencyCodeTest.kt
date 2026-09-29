package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class CurrencyCodeTest {
    @Test
    fun `accepts known ISO 4217 codes in uppercase`() {
        listOf("BRL", "USD", "JPY").forEach { assertEquals(it, CurrencyCode.parse(it).value) }
    }

    @Test
    fun `rejects lowercase, wrong length and unknown codes with invalid_currency`() {
        listOf("brl", "BR", "BRLL", "ZZZ", "", "B1L", " BRL").forEach { raw ->
            val error = assertFailsWith<InvalidEventException>("aceitou '$raw'") { CurrencyCode.parse(raw) }
            assertEquals(RejectionReason.INVALID_CURRENCY, error.reason, "com '$raw'")
        }
    }

    @Test
    fun `exposes the java currency for presentation rules`() {
        assertEquals(2, CurrencyCode.parse("BRL").currency.defaultFractionDigits)
        assertEquals(0, CurrencyCode.parse("JPY").currency.defaultFractionDigits)
    }

    @Test
    fun `equality is by code`() {
        assertEquals(CurrencyCode.parse("BRL"), CurrencyCode.parse("BRL"))
    }

    @Test
    fun `boxed currency codes in collections keep their code`() {
        val codes = listOf(CurrencyCode.parse("BRL"), CurrencyCode.parse("USD"))

        assertEquals(listOf("BRL", "USD"), codes.map { it.value })
        assertEquals(listOf("BRL", "USD"), codes.asAnyList().map { it.toString() })
    }

    private fun List<Any>.asAnyList(): List<Any> = this
}
