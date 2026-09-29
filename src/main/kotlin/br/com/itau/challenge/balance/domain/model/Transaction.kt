package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import java.math.BigDecimal

/**
 * Transacao do evento. [amount] e validado (>= 0, mesmas regras de precisao e escala de [Money]), mas nao e persistido:
 * nao ha padrao de acesso que o justifique.
 */
data class Transaction(
    val id: TransactionId,
    val type: TransactionType,
    val amount: BigDecimal,
    val currency: CurrencyCode,
    val status: TransactionStatus,
    val timestamp: EventInstant,
) {
    init {
        validTransactionAmount(amount)
    }

    /** Nunca expoe o valor: valores monetarios nao podem vazar em logs. */
    override fun toString(): String = "Transaction(id=$id, type=$type, status=$status)"

    companion object {
        /**
         * Valida o valor de uma transacao (precisao/escala de [validatedAmount] e `>= 0`) e devolve o valor normalizado ou
         * lanca `invalid_value`. Exposta para que a borda valide na ordem documentada dos campos.
         */
        fun validTransactionAmount(amount: BigDecimal): BigDecimal {
            val normalized = validatedAmount(amount)
            if (normalized.signum() < 0) throw InvalidEventException(RejectionReason.INVALID_VALUE)
            return normalized
        }
    }
}
