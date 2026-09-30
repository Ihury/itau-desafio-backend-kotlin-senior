package br.com.itau.challenge.balance.domain.exception

import br.com.itau.challenge.balance.domain.model.StoreFailureCause

/**
 * Consulta rejeitada sem chamar o armazenamento (circuit breaker da leitura aberto). E uma [BalanceStoreUnavailableException]
 * (503 + `Retry-After`), mas um desfecho esperado e frequente: sem pilha e logada em DEBUG.
 */
class BalanceStoreCircuitOpenException(
    cause: Throwable? = null,
) : BalanceStoreUnavailableException(StoreFailureCause.UNAVAILABLE, cause) {
    override fun fillInStackTrace(): Throwable = this
}
