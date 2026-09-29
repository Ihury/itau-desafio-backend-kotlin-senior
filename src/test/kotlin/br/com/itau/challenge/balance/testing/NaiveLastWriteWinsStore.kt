package br.com.itau.challenge.balance.testing

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import java.util.concurrent.ConcurrentHashMap

/**
 * Implementacao INGENUA de [BalanceSnapshotWriter]: "o ultimo a chegar vence". Nao compara precedencia, nao detecta
 * duplicado nem obsoleto. Existe apenas para demonstrar o vermelho da propriedade de convergencia (meta-teste): uma
 * propriedade que nao consegue reprovar esta implementacao nao prova nada sobre a ordem de chegada.
 */
class NaiveLastWriteWinsStore : BalanceSnapshotWriter {
    private val snapshots = ConcurrentHashMap<AccountId, BalanceSnapshot>()

    override fun applyIfNewer(snapshot: BalanceSnapshot): ApplyResult {
        snapshots[snapshot.accountId] = snapshot
        return ApplyResult.Applied
    }

    /** Snapshot vigente da conta (inspecao dos testes). */
    fun current(accountId: AccountId): BalanceSnapshot? = snapshots[accountId]
}
