package br.com.itau.challenge.balance.domain.exception

import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.domain.model.StoreFailureDetails

open class BalanceStoreUnavailableException(
    val failureCause: StoreFailureCause,
    cause: Throwable? = null,
    val details: StoreFailureDetails? = null,
) : RuntimeException("balance store unavailable: $failureCause", cause) {
    fun logDescription(): String = details?.let { "cause=$failureCause $it" } ?: "cause=$failureCause"
}
