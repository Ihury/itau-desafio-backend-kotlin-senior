package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import java.math.BigDecimal

/** Limite de digitos significativos (e de escala positiva): o tipo numerico `N` do DynamoDB aceita ate 38. */
private const val MAX_DIGITS = 38

/**
 * Regras de precisao e escala de qualquer valor monetario (saldo ou valor de transacao). A precisao e medida por
 * [BigDecimal.precision] do valor recebido (zeros a direita contam: conservador). Escala negativa (`1E+3`) e
 * expandida para escala zero somente se `precisao - escala <= 38`, sem materializar expoentes gigantes.
 * Devolve o valor normalizado ou lanca `invalid_value`.
 */
fun validatedAmount(amount: BigDecimal): BigDecimal {
    val scale = amount.scale().toLong()
    val precision = amount.precision().toLong()
    if (scale < 0) {
        if (precision - scale > MAX_DIGITS) throw InvalidEventException(RejectionReason.INVALID_VALUE)
        return amount.setScale(0)
    }
    if (precision > MAX_DIGITS || scale > MAX_DIGITS) throw InvalidEventException(RejectionReason.INVALID_VALUE)
    return amount
}

/**
 * Valor monetario exato: [BigDecimal] (nunca Double/Float) e [CurrencyCode]. Pode ser zero ou negativo.
 * Igualdade e hashCode por valor numerico: `183.10 == 183.1`, pois o DynamoDB normaliza a escala.
 */
class Money private constructor(
    val amount: BigDecimal,
    val currency: CurrencyCode,
) {
    /**
     * Valor para apresentacao: completa a escala ate as casas decimais padrao da moeda (BRL `183.1` -> `183.10`),
     * sem nunca arredondar. Moedas sem casas padrao definidas (`-1`, ex.: XAU) permanecem inalteradas.
     */
    fun paddedToCurrencyScale(): BigDecimal {
        val fractionDigits = currency.currency.defaultFractionDigits
        return if (fractionDigits < 0) amount else amount.setScale(maxOf(amount.scale(), fractionDigits))
    }

    override fun equals(other: Any?): Boolean =
        this === other || (other is Money && currency == other.currency && amount.compareTo(other.amount) == 0)

    override fun hashCode(): Int = 31 * amount.stripTrailingZeros().hashCode() + currency.hashCode()

    /** Nunca expoe o valor: saldos nao podem vazar em logs. */
    override fun toString(): String = "Money($currency)"

    companion object {
        fun of(
            amount: BigDecimal,
            currency: CurrencyCode,
        ): Money = Money(validatedAmount(amount), currency)
    }
}
