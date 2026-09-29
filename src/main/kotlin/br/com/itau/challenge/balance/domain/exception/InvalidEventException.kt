package br.com.itau.challenge.balance.domain.exception

import br.com.itau.challenge.balance.domain.model.RejectionReason

/**
 * Evento invalido, com o [reason] do catalogo. [detail] e, quando aplicavel, o caminho do campo
 * (ex.: `transaction.currency`); nunca recebe valores do payload. A mensagem e somente o codigo do motivo.
 */
class InvalidEventException(
    val reason: RejectionReason,
    val detail: String? = null,
) : RuntimeException(reason.code) {
    fun withDetail(path: String): InvalidEventException = InvalidEventException(reason, path)
}
