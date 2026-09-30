package br.com.itau.challenge.balance.application

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.TransactionEvent
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import br.com.itau.challenge.balance.port.output.ProcessingMetrics
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Clock

/**
 * O snapshot e a projecao do evento de maior precedencia (nunca uma soma de transacoes) e a arbitragem e feita atomicamente
 * pelo armazenamento. Contabiliza exatamente um desfecho por evento aplicado; falhas do armazenamento propagam intactas, sem
 * desfecho contabilizado. Nunca registra saldo nem titular.
 *
 * Antes de escrever, valida que `transaction.timestamp` e `account.created_at` nao passam de `agora + tolerancia`: o relogio
 * so valida e nunca participa da precedencia. A rejeicao lanca [InvalidEventException] (`invalid_timestamp`, com o caminho do
 * campo) sem tocar o armazenamento.
 */
@Service
class ProcessTransactionEventService(
    private val writer: BalanceSnapshotWriter,
    private val metrics: ProcessingMetrics,
    private val clock: Clock,
    private val futureTolerance: FutureTolerance,
) : ProcessTransactionEventUseCase {
    override fun process(event: TransactionEvent): ApplyResult {
        rejectFutureTimestamps(event)
        val snapshot = BalanceSnapshot.from(event)
        val result = writer.applyIfNewer(snapshot)
        val accountId = snapshot.accountId
        val transactionId = snapshot.precedence.transactionId
        when (result) {
            is ApplyResult.Applied -> {
                metrics.processed()
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

    /** `transaction.timestamp` e verificado antes de `account.created_at`: a ordem define o caminho quando ambos falham. */
    private fun rejectFutureTimestamps(event: TransactionEvent) {
        val limit = latestAcceptableMicros()
        if (event.transaction.timestamp.micros > limit) throw futureTimestampRejection("transaction.timestamp")
        if (event.account.createdAt.micros > limit) throw futureTimestampRejection("account.created_at")
    }

    private fun latestAcceptableMicros(): Long {
        val latest = clock.instant().plus(futureTolerance.duration)
        return Math.addExact(Math.multiplyExact(latest.epochSecond, MICROS_PER_SECOND), latest.nano / NANOS_PER_MICRO)
    }

    private fun futureTimestampRejection(path: String) = InvalidEventException(RejectionReason.INVALID_TIMESTAMP, path)

    private companion object {
        private const val MICROS_PER_SECOND = 1_000_000L
        private const val NANOS_PER_MICRO = 1_000L
        private val log = LoggerFactory.getLogger(ProcessTransactionEventService::class.java)
    }
}
