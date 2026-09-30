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
        fieldPath: String?,
    ): Headers {
        val headers = RecordHeaders()
        headers.add(REASON_HEADER, reason.code.toByteArray(Charsets.UTF_8))
        if (fieldPath != null) headers.add(FIELD_PATH_HEADER, fieldPath.toByteArray(Charsets.UTF_8))
        headers.add(REJECTED_AT_HEADER, clock.instant().toString().toByteArray(Charsets.UTF_8))
        return headers
    }

    companion object {
        const val REASON_HEADER = "x-rejection-reason"
        const val FIELD_PATH_HEADER = "x-rejection-detail"
        const val REJECTED_AT_HEADER = "x-rejected-at"
    }
}
