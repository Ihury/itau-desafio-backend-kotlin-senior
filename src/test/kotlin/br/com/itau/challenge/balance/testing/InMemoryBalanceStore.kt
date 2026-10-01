package br.com.itau.challenge.balance.testing

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotReader
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import java.util.concurrent.ConcurrentHashMap

class InMemoryBalanceStore :
    BalanceSnapshotReader,
    BalanceSnapshotWriter {
    private val snapshots = ConcurrentHashMap<AccountId, BalanceSnapshot>()

    @Volatile
    private var readFailure: RuntimeException? = null

    @Volatile
    private var writeFailure: RuntimeException? = null

    fun seedWithoutArbitration(snapshot: BalanceSnapshot) {
        snapshots[snapshot.accountId] = snapshot
    }

    fun peek(accountId: AccountId): BalanceSnapshot? = snapshots[accountId]

    fun failReadsWith(exception: RuntimeException) {
        readFailure = exception
    }

    fun failWritesWith(exception: RuntimeException) {
        writeFailure = exception
    }

    fun recoverReads() {
        readFailure = null
    }

    fun recoverWrites() {
        writeFailure = null
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
                    result = ApplyResult.Duplicate(conflicting = hasDivergentContent(snapshot, current))
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

    private fun hasDivergentContent(
        candidate: BalanceSnapshot,
        current: BalanceSnapshot,
    ): Boolean = candidate.ownerId != current.ownerId || candidate.status != current.status || candidate.balance != current.balance
}
