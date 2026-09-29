package br.com.itau.challenge.balance.application

import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.TransactionEvent
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import br.com.itau.challenge.balance.port.output.ProcessingMetrics
import org.slf4j.LoggerFactory

/**
 * Ingestao de um evento: o snapshot e a projecao do evento de maior precedencia (nunca uma soma de transacoes) e a
 * arbitragem e feita atomicamente pelo armazenamento. Contabiliza exatamente um desfecho por evento aplicado; falhas do
 * armazenamento propagam intactas, sem desfecho contabilizado. Nunca registra saldo nem titular.
 *
 * O `@Service` entra na unidade C16, junto com o bean do [BalanceSnapshotWriter]: antes disso o contexto Spring nao teria
 * como satisfazer a dependencia e a suite ficaria vermelha.
 */
class ProcessTransactionEventService(
    private val writer: BalanceSnapshotWriter,
    private val metrics: ProcessingMetrics,
) : ProcessTransactionEventUseCase {
    override fun process(event: TransactionEvent): ApplyResult {
        val snapshot = BalanceSnapshot.from(event)
        val result = writer.applyIfNewer(snapshot)
        val accountId = snapshot.accountId
        val transactionId = snapshot.precedence.transactionId
        when (result) {
            is ApplyResult.Applied -> {
                metrics.applied()
                log.info("event applied accountId={} transactionId={}", accountId, transactionId)
            }
            is ApplyResult.Obsolete -> {
                metrics.obsolete()
                log.debug("event obsolete accountId={} transactionId={}", accountId, transactionId)
            }
            is ApplyResult.Duplicate -> {
                metrics.duplicate(result.conflicting)
                if (result.conflicting) {
                    log.warn("conflicting duplicate event accountId={} transactionId={}", accountId, transactionId)
                } else {
                    log.debug("duplicate event accountId={} transactionId={}", accountId, transactionId)
                }
            }
        }
        return result
    }

    private companion object {
        private val log = LoggerFactory.getLogger(ProcessTransactionEventService::class.java)
    }
}
