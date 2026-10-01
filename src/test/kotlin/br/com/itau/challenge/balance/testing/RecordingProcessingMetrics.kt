package br.com.itau.challenge.balance.testing

import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.port.output.ConsumerFailureMetrics
import br.com.itau.challenge.balance.port.output.OutcomeMetrics
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/** Duble de [OutcomeMetrics] e [ConsumerFailureMetrics] que registra, em ordem, cada desfecho contabilizado. */
class RecordingProcessingMetrics :
    OutcomeMetrics,
    ConsumerFailureMetrics {
    private val outcomeLog = CopyOnWriteArrayList<String>()

    private val dltFailures = AtomicInteger()

    /** Falhas de publicacao no DLT (nao sao desfechos). */
    val dltPublishFailures: Int get() = dltFailures.get()

    private val backpressureLog = CopyOnWriteArrayList<StoreFailureCause>()

    /** Causas de cada pausa por backpressure, na ordem (nao sao desfechos). */
    val backpressureCauses: List<StoreFailureCause> get() = backpressureLog.toList()

    /** Desfechos na ordem: `processed`, `obsolete`, `duplicate`, `duplicate(conflicting)` ou `rejected(<codigo>)`. */
    val outcomes: List<String> get() = outcomeLog.toList()

    override fun record(result: ApplyResult) {
        outcomeLog +=
            when (result) {
                is ApplyResult.Applied -> "processed"
                is ApplyResult.Obsolete -> "obsolete"
                is ApplyResult.Duplicate -> if (result.conflicting) "duplicate(conflicting)" else "duplicate"
            }
    }

    override fun rejected(reason: RejectionReason) {
        outcomeLog += "rejected(${reason.code})"
    }

    override fun dltPublishFailed() {
        dltFailures.incrementAndGet()
    }

    override fun backpressure(cause: StoreFailureCause) {
        backpressureLog += cause
    }
}
