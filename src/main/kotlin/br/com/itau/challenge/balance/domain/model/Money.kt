package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import java.math.BigDecimal

private const val DYNAMODB_NUMBER_MAX_DIGITS = 38

fun validatedAmount(amount: BigDecimal): BigDecimal {
    val scale = amount.scale().toLong()
    val precision = amount.precision().toLong()
    if (scale < 0) {
        if (precision - scale > DYNAMODB_NUMBER_MAX_DIGITS) throw InvalidEventException(RejectionReason.INVALID_VALUE)
        return amount.setScale(0)
    }
    if (precision > DYNAMODB_NUMBER_MAX_DIGITS || scale > DYNAMODB_NUMBER_MAX_DIGITS) throw InvalidEventException(RejectionReason.INVALID_VALUE)
    return amount
}

class Money private constructor(
    val amount: BigDecimal,
    val currency: CurrencyCode,
) {
    fun paddedToCurrencyScale(): BigDecimal {
        val fractionDigits = currency.currency.defaultFractionDigits
        return if (fractionDigits < 0) amount else amount.setScale(maxOf(amount.scale(), fractionDigits))
    }

    override fun equals(other: Any?): Boolean =
        this === other || (other is Money && currency == other.currency && amount.compareTo(other.amount) == 0)

    override fun hashCode(): Int = 31 * amount.stripTrailingZeros().hashCode() + currency.hashCode()

    override fun toString(): String = "Money($currency)"

    companion object {
        fun of(
            amount: BigDecimal,
            currency: CurrencyCode,
        ): Money = Money(validatedAmount(amount), currency)
    }
}
