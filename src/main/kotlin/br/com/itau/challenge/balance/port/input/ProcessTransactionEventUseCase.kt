package br.com.itau.challenge.balance.port.input

import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.TransactionEvent

/** Aplica um evento de transacao ja validado ao snapshot da conta (ingestao). */
interface ProcessTransactionEventUseCase {
    /**
     * Mantem o snapshot da conta como a projecao do evento de maior precedencia e contabiliza o desfecho.
     *
     * @throws br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException armazenamento indisponivel (transitoria; nenhum desfecho contabilizado)
     * @throws br.com.itau.challenge.balance.domain.exception.BalanceStoreRejectedException o armazenamento rejeitou a escrita (nenhum desfecho contabilizado)
     */
    fun process(event: TransactionEvent): ApplyResult
}
