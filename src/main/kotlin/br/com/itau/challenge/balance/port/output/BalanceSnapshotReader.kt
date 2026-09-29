package br.com.itau.challenge.balance.port.output

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot

/** Leitura do snapshot vigente de uma conta no armazenamento. */
interface BalanceSnapshotReader {
    /**
     * Devolve o snapshot vigente ou `null` se a conta nao tem snapshot.
     *
     * @throws br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException qualquer falha do armazenamento
     *   (jamais `null` em caso de falha)
     */
    fun find(accountId: AccountId): BalanceSnapshot?
}
