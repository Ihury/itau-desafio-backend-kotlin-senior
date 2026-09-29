package br.com.itau.challenge.balance.port.output

import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot

/** Escrita do snapshot no armazenamento: a arbitragem de precedencia e atomica e acontece no proprio armazenamento. */
interface BalanceSnapshotWriter {
    /**
     * Grava [snapshot] somente se ele superar o vigente (ou se a conta ainda nao tem snapshot).
     *
     * @throws br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException falha transitoria do armazenamento
     * @throws br.com.itau.challenge.balance.domain.exception.BalanceStoreRejectedException o armazenamento rejeitou a escrita
     */
    fun applyIfNewer(snapshot: BalanceSnapshot): ApplyResult
}
