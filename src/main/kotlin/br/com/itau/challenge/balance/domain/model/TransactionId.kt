package br.com.itau.challenge.balance.domain.model

/** Identificador UUID em forma canonica (minusculas). Construido somente por [parse]. */
@JvmInline
value class TransactionId private constructor(
    private val canonical: String,
) {
    val value: String get() = canonical

    override fun toString(): String = canonical

    companion object {
        fun parse(raw: String): TransactionId = TransactionId(canonicalUuid(raw))
    }
}
