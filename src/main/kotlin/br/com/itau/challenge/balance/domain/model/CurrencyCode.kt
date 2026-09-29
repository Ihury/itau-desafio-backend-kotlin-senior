package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import java.util.Currency

/** Codigo de moeda ISO 4217: tres letras maiusculas conhecidas por [Currency]. Construido somente por [parse]. */
@JvmInline
value class CurrencyCode private constructor(
    private val code: String,
) {
    val value: String get() = code

    val currency: Currency get() = Currency.getInstance(code)

    override fun toString(): String = code

    companion object {
        private val SHAPE = Regex("^[A-Z]{3}$")
        private val known: Set<String> = Currency.getAvailableCurrencies().map { it.currencyCode }.toSet()

        fun parse(raw: String): CurrencyCode {
            if (!SHAPE.matches(raw) || raw !in known) throw InvalidEventException(RejectionReason.INVALID_CURRENCY)
            return CurrencyCode(raw)
        }
    }
}
