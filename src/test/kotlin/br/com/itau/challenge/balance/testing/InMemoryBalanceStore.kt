package br.com.itau.challenge.balance.testing

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotReader
import java.util.concurrent.ConcurrentHashMap

/**
 * Fake em memoria do armazenamento de snapshots (dublê de teste, sem infraestrutura).
 * Parte de leitura: [BalanceSnapshotReader]. A parte de escrita entra com a ingestao.
 */
class InMemoryBalanceStore : BalanceSnapshotReader {
    private val snapshots = ConcurrentHashMap<AccountId, BalanceSnapshot>()

    @Volatile
    private var readFailure: RuntimeException? = null

    /** Grava (ou substitui) o snapshot da conta, sem arbitragem de precedencia. */
    fun seed(snapshot: BalanceSnapshot) {
        snapshots[snapshot.accountId] = snapshot
    }

    /** Toda leitura seguinte lanca [exception]; `null` restaura o comportamento normal. */
    fun failReadsWith(exception: RuntimeException?) {
        readFailure = exception
    }

    override fun find(accountId: AccountId): BalanceSnapshot? {
        readFailure?.let { throw it }
        return snapshots[accountId]
    }
}
