package br.com.itau.challenge.balance.application

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.TransactionEvent
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import br.com.itau.challenge.balance.port.output.OutcomeMetrics
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Clock

@Service
class ProcessTransactionEventService(
    private val writer: BalanceSnapshotWriter,
    private val metrics: OutcomeMetrics,
    private val clock: Clock,
    private val futureTolerance: FutureTolerance,
) : ProcessTransactionEventUseCase {
    override fun process(event: TransactionEvent): ApplyResult {
        rejectFutureTimestamps(event)
        val snapshot = BalanceSnapshot.from(event)
        val result = writer.applyIfNewer(snapshot)
        metrics.record(result)
        logOutcome(snapshot, result)
        return result
    }

    private fun rejectFutureTimestamps(event: TransactionEvent) {
        val limit = clock.instant().plus(futureTolerance.duration)
        if (event.transaction.timestamp.isAfter(limit)) throw futureTimestampRejection("transaction.timestamp")
        if (event.account.createdAt.isAfter(limit)) throw futureTimestampRejection("account.created_at")
    }

    private fun futureTimestampRejection(path: String) = InvalidEventException(RejectionReason.INVALID_TIMESTAMP, path)

    private fun logOutcome(
        snapshot: BalanceSnapshot,
        result: ApplyResult,
    ) {
        val accountId = snapshot.accountId
        val transactionId = snapshot.precedence.transactionId
        when (result) {
            is ApplyResult.Applied -> log.info("event applied accountId={} transactionId={}", accountId, transactionId)
            is ApplyResult.Obsolete -> log.debug("event obsolete accountId={} transactionId={}", accountId, transactionId)
            is ApplyResult.Duplicate ->
                if (result.conflicting) {
                    log.warn("conflicting duplicate event accountId={} transactionId={}", accountId, transactionId)
                } else {
                    log.debug("duplicate event accountId={} transactionId={}", accountId, transactionId)
                }
        }
    }

    private companion object {
        private val log = LoggerFactory.getLogger(ProcessTransactionEventService::class.java)
    }
}
