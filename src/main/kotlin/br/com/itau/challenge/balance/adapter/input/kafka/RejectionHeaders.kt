package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.model.RejectionReason
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import java.time.Clock

/**
 * Headers de rejeicao da mensagem no DLT (contracts/kafka-events.md secao 5): o codigo do motivo, o caminho do campo (so
 * quando atribuivel a um campo) e o instante do isolamento (ISO 8601 UTC, do [Clock] injetado). NUNCA carregam valores do
 * payload nem texto de excecao.
 */
class RejectionHeaders(
    private val clock: Clock,
) {
    fun of(
        reason: RejectionReason,
        detail: String?,
    ): Headers {
        val headers = RecordHeaders()
        headers.add(REASON, reason.code.toByteArray(Charsets.UTF_8))
        if (detail != null) headers.add(DETAIL, detail.toByteArray(Charsets.UTF_8))
        headers.add(REJECTED_AT, clock.instant().toString().toByteArray(Charsets.UTF_8))
        return headers
    }

    companion object {
        const val REASON = "x-rejection-reason"
        const val DETAIL = "x-rejection-detail"
        const val REJECTED_AT = "x-rejected-at"
    }
}
