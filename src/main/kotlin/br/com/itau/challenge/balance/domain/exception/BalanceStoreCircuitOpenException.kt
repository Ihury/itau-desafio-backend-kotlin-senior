package br.com.itau.challenge.balance.domain.exception

import br.com.itau.challenge.balance.domain.model.StoreFailureCause

class BalanceStoreCircuitOpenException(
    cause: Throwable? = null,
) : BalanceStoreUnavailableException(StoreFailureCause.UNAVAILABLE, cause) {
    override fun fillInStackTrace(): Throwable = this
}
