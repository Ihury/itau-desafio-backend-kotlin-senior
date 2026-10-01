package br.com.itau.challenge.balance.testing

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import java.util.concurrent.ConcurrentHashMap

class NaiveLastWriteWinsStore : BalanceSnapshotWriter {
    private val snapshots = ConcurrentHashMap<AccountId, BalanceSnapshot>()

    override fun applyIfNewer(snapshot: BalanceSnapshot): ApplyResult {
        snapshots[snapshot.accountId] = snapshot
        return ApplyResult.Applied
    }

    fun peek(accountId: AccountId): BalanceSnapshot? = snapshots[accountId]
}
