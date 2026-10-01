package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidEventException

private val CANONICAL_UUID =
    Regex("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

internal fun canonicalUuid(raw: String): String {
    if (!CANONICAL_UUID.matches(raw)) throw InvalidEventException(RejectionReason.INVALID_IDENTIFIER)
    return raw.lowercase()
}
