package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidEventException

/** Comparacao exata e sensivel a maiusculas; qualquer outro valor e `unknown_domain_value`. */
enum class TransactionStatus {
    APPROVED,
    DECLINED,
    ;

    companion object {
        fun parse(raw: String): TransactionStatus =
            entries.firstOrNull { it.name == raw } ?: throw InvalidEventException(RejectionReason.UNKNOWN_DOMAIN_VALUE)
    }
}
