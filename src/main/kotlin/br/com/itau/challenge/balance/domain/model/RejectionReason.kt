package br.com.itau.challenge.balance.domain.model

/**
 * Catalogo enumerado e estavel dos motivos de rejeicao de uma mensagem (FR-016). O [code] e o valor
 * publicado no header `x-rejection-reason` e na tag `reason` das metricas: nunca deve mudar.
 */
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
