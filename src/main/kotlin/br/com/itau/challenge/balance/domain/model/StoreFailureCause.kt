package br.com.itau.challenge.balance.domain.model

/** Classificacao de uma falha transitoria do armazenamento (tag `cause` das metricas de backpressure). */
enum class StoreFailureCause {
    THROTTLED,
    UNAVAILABLE,
    TIMEOUT,
}
