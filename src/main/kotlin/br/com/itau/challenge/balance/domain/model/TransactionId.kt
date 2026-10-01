package br.com.itau.challenge.balance.domain.model

@JvmInline
value class TransactionId private constructor(
    val value: String,
) {
    override fun toString(): String = value

    companion object {
        fun parse(raw: String): TransactionId = TransactionId(canonicalUuid(raw))
    }
}
