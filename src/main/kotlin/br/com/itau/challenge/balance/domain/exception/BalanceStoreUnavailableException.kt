package br.com.itau.challenge.balance.domain.exception

import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.domain.model.StoreFailureDetails

/**
 * Armazenamento indisponivel (falha transitoria). [failureCause] e a classificacao; `cause` e a excecao original do SDK e
 * [details] o diagnostico seguro para log (sem a mensagem livre do SDK).
 */
open class BalanceStoreUnavailableException(
    val failureCause: StoreFailureCause,
    cause: Throwable? = null,
    val details: StoreFailureDetails? = null,
) : RuntimeException("balance store unavailable: $failureCause", cause) {
    /** `cause=<causa>` seguido do diagnostico do SDK, quando ha. */
    fun logDescription(): String = details?.let { "cause=$failureCause $it" } ?: "cause=$failureCause"
}
