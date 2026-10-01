package br.com.itau.challenge.balance.domain.model

enum class RejectionReason(
    val code: String,
) {
    MALFORMED_PAYLOAD("malformed_payload"),
    MISSING_FIELD("missing_field"),
    INVALID_IDENTIFIER("invalid_identifier"),
    INVALID_VALUE("invalid_value"),
    INVALID_CURRENCY("invalid_currency"),
    INVALID_TIMESTAMP("invalid_timestamp"),
    UNKNOWN_DOMAIN_VALUE("unknown_domain_value"),
    UNPROCESSABLE_EVENT("unprocessable_event"),
    ;

    companion object {
        private val byCode = entries.associateBy { it.code }

        fun fromCode(code: String): RejectionReason? = byCode[code]
    }
}
