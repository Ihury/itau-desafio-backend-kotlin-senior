package br.com.itau.challenge.balance.adapter.input.kafka

import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import java.time.Clock

class RejectionHeaders(
    private val clock: Clock,
) {
    fun of(rejection: Rejection): Headers {
        val headers = RecordHeaders()
        headers.add(REASON_HEADER, rejection.reason.code.utf8())
        rejection.fieldPath?.let { headers.add(FIELD_PATH_HEADER, it.utf8()) }
        headers.add(REJECTED_AT_HEADER, clock.instant().toString().utf8())
        return headers
    }

    private fun String.utf8(): ByteArray = toByteArray(Charsets.UTF_8)

    companion object {
        const val REASON_HEADER = "x-rejection-reason"
        const val FIELD_PATH_HEADER = "x-rejection-detail"
        const val REJECTED_AT_HEADER = "x-rejected-at"
    }
}
