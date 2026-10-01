package br.com.itau.challenge.balance.application

import br.com.itau.challenge.balance.domain.exception.AccountDisabledException
import br.com.itau.challenge.balance.domain.exception.AccountNotFoundException
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.input.GetBalanceUseCase
import br.com.itau.challenge.balance.port.output.BalanceSnapshotReader
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class GetBalanceService(
    private val reader: BalanceSnapshotReader,
) : GetBalanceUseCase {
    override fun getBalance(accountId: AccountId): BalanceSnapshot {
        val snapshot =
            reader.find(accountId)
                ?: run {
                    log.debug("balance query: account not found accountId={}", accountId)
                    throw AccountNotFoundException(accountId)
                }
        if (snapshot.isDisabled()) {
            log.debug("balance query: account disabled accountId={}", accountId)
            throw AccountDisabledException(accountId)
        }
        return snapshot
    }

    private companion object {
        private val log = LoggerFactory.getLogger(GetBalanceService::class.java)
    }
}
