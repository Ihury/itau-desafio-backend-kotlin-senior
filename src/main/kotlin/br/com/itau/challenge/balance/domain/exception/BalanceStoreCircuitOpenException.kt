package br.com.itau.challenge.balance.domain.exception

import br.com.itau.challenge.balance.domain.model.StoreFailureCause

/**
 * Consulta rejeitada SEM chamar o armazenamento porque o circuit breaker da leitura esta aberto (falha rapida). Continua sendo
 * [BalanceStoreUnavailableException] para quem a trata (503 + `Retry-After`), mas nao e uma falha real do armazenamento: e um
 * desfecho esperado e frequente enquanto o circuito esta aberto, entao nao preenche pilha e o log a trata em DEBUG.
 */
class BalanceStoreCircuitOpenException(
    cause: Throwable? = null,
) : BalanceStoreUnavailableException(StoreFailureCause.UNAVAILABLE, cause) {
    /** Sem pilha: a rejeicao acontece por requisicao e a pilha so custaria CPU e ruido. */
    override fun fillInStackTrace(): Throwable = this
}
