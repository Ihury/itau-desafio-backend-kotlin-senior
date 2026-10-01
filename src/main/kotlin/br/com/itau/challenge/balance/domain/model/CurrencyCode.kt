package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import java.util.Currency

@JvmInline
value class CurrencyCode private constructor(
    private val code: String,
) {
    val value: String get() = code

    val currency: Currency get() = Currency.getInstance(code)

    override fun toString(): String = code

    companion object {
        private val THREE_UPPERCASE_LETTERS = Regex("^[A-Z]{3}$")
        private val KNOWN_CODES: Set<String> = Currency.getAvailableCurrencies().map { it.currencyCode }.toSet()

        fun parse(raw: String): CurrencyCode {
            if (!THREE_UPPERCASE_LETTERS.matches(raw) || raw !in KNOWN_CODES) throw InvalidEventException(RejectionReason.INVALID_CURRENCY)
            return CurrencyCode(raw)
        }
    }
}
