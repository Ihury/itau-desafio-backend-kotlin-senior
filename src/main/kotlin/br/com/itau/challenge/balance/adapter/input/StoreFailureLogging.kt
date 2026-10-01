package br.com.itau.challenge.balance.adapter.input

import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import org.slf4j.Logger

internal fun Logger.logStoreUnavailable(
    failure: BalanceStoreUnavailableException,
    message: String,
    vararg arguments: Any?,
) {
    if (failure.failureCause == StoreFailureCause.MISCONFIGURED) error(message, *arguments) else warn(message, *arguments)
}
