package br.com.itau.challenge.balance.port.output

enum class IngestOutcome {
    PROCESSED,
    OBSOLETE,
    DUPLICATE,
    REJECTED,
    ERROR,
}

interface IngestMetrics {
    fun ingestDuration(
        outcome: IngestOutcome,
        nanos: Long,
    )
}
