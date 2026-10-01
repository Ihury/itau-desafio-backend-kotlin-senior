package br.com.itau.challenge.balance.port.output

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot

interface BalanceSnapshotReader {
    fun find(accountId: AccountId): BalanceSnapshot?
}
