package br.com.itau.challenge.balance.domain.model

enum class AccountStatus {
    ENABLED,
    DISABLED,
    ;

    companion object {
        fun parse(raw: String): AccountStatus = parseEnum(raw)
    }
}
