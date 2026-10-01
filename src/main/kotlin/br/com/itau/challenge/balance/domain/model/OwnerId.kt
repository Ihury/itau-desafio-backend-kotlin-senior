package br.com.itau.challenge.balance.domain.model

@JvmInline
value class OwnerId private constructor(
    val value: String,
) {
    override fun toString(): String = value

    companion object {
        fun parse(raw: String): OwnerId = OwnerId(canonicalUuid(raw))
    }
}
