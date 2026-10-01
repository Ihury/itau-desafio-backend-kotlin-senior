package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import java.math.BigDecimal

data class Transaction(
    val id: TransactionId,
    val type: TransactionType,
    val amount: BigDecimal,
    val currency: CurrencyCode,
    val status: TransactionStatus,
    val timestamp: EventInstant,
) {
    init {
        validatedTransactionAmount(amount)
    }

    override fun toString(): String = "Transaction(id=$id, type=$type, status=$status)"

    companion object {
        fun validatedTransactionAmount(amount: BigDecimal): BigDecimal {
            val normalized = validatedAmount(amount)
            if (normalized.signum() < 0) throw InvalidEventException(RejectionReason.INVALID_VALUE)
            return normalized
        }
    }
}
