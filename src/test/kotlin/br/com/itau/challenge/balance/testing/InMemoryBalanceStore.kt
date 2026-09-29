package br.com.itau.challenge.balance.testing

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotReader
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import java.util.concurrent.ConcurrentHashMap

/**
 * Fake em memoria do armazenamento de snapshots (dublê de teste, sem infraestrutura), com a mesma semantica do
 * DynamoDB: a arbitragem de precedencia e atomica por conta (`ConcurrentHashMap.compute`) e usa
 * [BalanceSnapshot.supersedes]. Leitura: [BalanceSnapshotReader]; escrita: [BalanceSnapshotWriter].
 */
class InMemoryBalanceStore :
    BalanceSnapshotReader,
    BalanceSnapshotWriter {
    private val snapshots = ConcurrentHashMap<AccountId, BalanceSnapshot>()

    @Volatile
    private var readFailure: RuntimeException? = null

    @Volatile
    private var writeFailure: RuntimeException? = null

    /** Grava (ou substitui) o snapshot da conta, sem arbitragem de precedencia. */
    fun seed(snapshot: BalanceSnapshot) {
        snapshots[snapshot.accountId] = snapshot
    }

    /** Snapshot vigente da conta (inspecao dos testes; nao passa pela falha injetada). */
    fun current(accountId: AccountId): BalanceSnapshot? = snapshots[accountId]

    /** Toda leitura seguinte lanca [exception]; `null` restaura o comportamento normal. */
    fun failReadsWith(exception: RuntimeException?) {
        readFailure = exception
    }

    /** Toda escrita seguinte lanca [exception]; `null` restaura o comportamento normal. */
    fun failWritesWith(exception: RuntimeException?) {
        writeFailure = exception
    }

    override fun find(accountId: AccountId): BalanceSnapshot? {
        readFailure?.let { throw it }
        return snapshots[accountId]
    }

    override fun applyIfNewer(snapshot: BalanceSnapshot): ApplyResult {
        writeFailure?.let { throw it }
        var result: ApplyResult = ApplyResult.Applied
        snapshots.compute(snapshot.accountId) { _, current ->
            when {
                snapshot.supersedes(current) -> {
                    result = ApplyResult.Applied
                    snapshot
                }
                current != null && snapshot.precedence == current.precedence -> {
                    result = ApplyResult.Duplicate(conflicting = diverges(snapshot, current))
                    current
                }
                else -> {
                    result = ApplyResult.Obsolete
                    current
                }
            }
        }
        return result
    }

    /** Conteudo divergente de um mesmo evento (data-model 4.4): titular, situacao ou saldo (valor e moeda). */
    private fun diverges(
        candidate: BalanceSnapshot,
        current: BalanceSnapshot,
    ): Boolean = candidate.ownerId != current.ownerId || candidate.status != current.status || candidate.balance != current.balance
}
