package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidEventException

/** Tipo da transacao. Comparacao exata e sensivel a maiusculas (`unknown_domain_value` para qualquer outro valor). */
enum class TransactionType {
    CREDIT,
    DEBIT,
    ;

    companion object {
        fun parse(raw: String): TransactionType =
            entries.firstOrNull { it.name == raw } ?: throw InvalidEventException(RejectionReason.UNKNOWN_DOMAIN_VALUE)
    }
}
