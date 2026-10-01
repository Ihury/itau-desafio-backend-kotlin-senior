package br.com.itau.challenge.balance.port.output

import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot

interface BalanceSnapshotWriter {
    fun applyIfNewer(snapshot: BalanceSnapshot): ApplyResult
}
