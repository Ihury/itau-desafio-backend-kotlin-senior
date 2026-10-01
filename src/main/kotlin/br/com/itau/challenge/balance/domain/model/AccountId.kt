package br.com.itau.challenge.balance.domain.model

@JvmInline
value class AccountId private constructor(
    val value: String,
) {
    override fun toString(): String = value

    companion object {
        fun parse(raw: String): AccountId = AccountId(canonicalUuid(raw))
    }
}
