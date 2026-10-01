package br.com.itau.challenge.balance.testing

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter

class InMemoryBalanceStoreContractTest : BalanceSnapshotWriterContract() {
    private val store = InMemoryBalanceStore()

    override val writer: BalanceSnapshotWriter = store

    override fun currentStoredSnapshotOf(accountId: AccountId): BalanceSnapshot? = store.peek(accountId)
}
