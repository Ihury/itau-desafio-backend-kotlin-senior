package br.com.itau.challenge.balance.domain.model

/** UUID em forma canonica (minusculas). */
@JvmInline
value class OwnerId private constructor(
    private val canonical: String,
) {
    val value: String get() = canonical

    override fun toString(): String = canonical

    companion object {
        fun parse(raw: String): OwnerId = OwnerId(canonicalUuid(raw))
    }
}
