package br.com.itau.challenge.balance.testing

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter

/** O fake em memoria obedece ao mesmo contrato do escritor real (sem infraestrutura). */
class InMemoryBalanceStoreContractTest : BalanceSnapshotWriterContract() {
    private val store = InMemoryBalanceStore()

    override val writer: BalanceSnapshotWriter = store

    override fun currentOf(accountId: AccountId): BalanceSnapshot? = store.current(accountId)
}
