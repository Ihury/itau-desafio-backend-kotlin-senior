package br.com.itau.challenge.balance.domain.model

/**
 * Classificacao de uma falha transitoria do armazenamento (tag `cause` das metricas de backpressure). [MISCONFIGURED] (tabela
 * inexistente, acesso negado, credencial ausente, invalida ou expirada) so muda o DIAGNOSTICO: o tratamento continua transitorio
 * (reentrega sem limite, nunca DLT), pois a correcao e operacional e a mensagem valida nao pode ser descartada.
 */
enum class StoreFailureCause {
    THROTTLED,
    UNAVAILABLE,
    TIMEOUT,
    MISCONFIGURED,
}
