package br.com.itau.challenge.balance.testing

import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.port.output.ProcessingMetrics
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/** Dublê de [ProcessingMetrics] que registra, em ordem, cada desfecho contabilizado. */
class RecordingProcessingMetrics : ProcessingMetrics {
    private val recorded = CopyOnWriteArrayList<String>()

    private val dltFailures = AtomicInteger()

    /** Falhas de publicacao no DLT (nao sao desfechos). */
    val dltPublishFailures: Int get() = dltFailures.get()

    private val backpressure = CopyOnWriteArrayList<StoreFailureCause>()

    /** Causas de cada pausa por backpressure, na ordem (nao sao desfechos). */
    val backpressureCauses: List<StoreFailureCause> get() = backpressure.toList()

    /** Desfechos na ordem: `applied`, `obsolete`, `duplicate`, `duplicate(conflicting)` ou `rejected(<codigo>)`. */
    val outcomes: List<String> get() = recorded.toList()

    override fun applied() {
        recorded += "applied"
    }

    override fun obsolete() {
        recorded += "obsolete"
    }

    override fun duplicate(conflicting: Boolean) {
        recorded += if (conflicting) "duplicate(conflicting)" else "duplicate"
    }

    override fun rejected(reason: RejectionReason) {
        recorded += "rejected(${reason.code})"
    }

    override fun dltPublishFailed() {
        dltFailures.incrementAndGet()
    }

    override fun backpressure(cause: StoreFailureCause) {
        backpressure += cause
    }
}
