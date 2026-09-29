package br.com.itau.challenge.balance.domain.exception

import br.com.itau.challenge.balance.domain.model.StoreFailureCause

/**
 * Armazenamento indisponivel (falha transitoria). [failureCause] e a classificacao; `cause` e a excecao original do SDK.
 */
class BalanceStoreUnavailableException(
    val failureCause: StoreFailureCause,
    cause: Throwable? = null,
) : RuntimeException("balance store unavailable: $failureCause", cause)
