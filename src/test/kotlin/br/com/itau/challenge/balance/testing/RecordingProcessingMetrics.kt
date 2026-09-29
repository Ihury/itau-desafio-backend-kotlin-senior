package br.com.itau.challenge.balance.testing

import br.com.itau.challenge.balance.port.output.ProcessingMetrics
import java.util.concurrent.CopyOnWriteArrayList

/** Dublê de [ProcessingMetrics] que registra, em ordem, cada desfecho contabilizado. */
class RecordingProcessingMetrics : ProcessingMetrics {
    private val recorded = CopyOnWriteArrayList<String>()

    /** Desfechos na ordem: `applied`, `obsolete`, `duplicate` ou `duplicate(conflicting)`. */
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
}
