package br.com.itau.challenge.balance.port.input

import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.TransactionEvent

/** Aplica um evento de transacao ao snapshot da conta. */
interface ProcessTransactionEventUseCase {
    /**
     * @throws br.com.itau.challenge.balance.domain.exception.InvalidEventException `transaction.timestamp` ou `account.created_at` alem de agora + tolerancia (nenhum desfecho contabilizado)
     * @throws br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException armazenamento indisponivel (transitoria; nenhum desfecho contabilizado)
     * @throws br.com.itau.challenge.balance.domain.exception.BalanceStoreRejectedException o armazenamento rejeitou a escrita (nenhum desfecho contabilizado)
     */
    fun process(event: TransactionEvent): ApplyResult
}
