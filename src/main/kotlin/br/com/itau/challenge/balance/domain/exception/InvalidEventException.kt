package br.com.itau.challenge.balance.domain.exception

import br.com.itau.challenge.balance.domain.model.RejectionReason

class InvalidEventException(
    val reason: RejectionReason,
    val fieldPath: String? = null,
) : RuntimeException(reason.code) {
    fun withFieldPath(path: String): InvalidEventException = InvalidEventException(reason, path)
}
