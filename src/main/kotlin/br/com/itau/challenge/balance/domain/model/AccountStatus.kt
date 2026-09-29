package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidEventException

/** Situacao da conta. Comparacao exata e sensivel a maiusculas (`unknown_domain_value` para qualquer outro valor). */
enum class AccountStatus {
    ENABLED,
    DISABLED,
    ;

    companion object {
        fun parse(raw: String): AccountStatus =
            entries.firstOrNull { it.name == raw } ?: throw InvalidEventException(RejectionReason.UNKNOWN_DOMAIN_VALUE)
    }
}
