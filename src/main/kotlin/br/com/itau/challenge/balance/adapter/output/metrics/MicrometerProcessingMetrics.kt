package br.com.itau.challenge.balance.adapter.output.metrics

import br.com.itau.challenge.balance.port.output.ProcessingMetrics
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component

/**
 * Contadores de desfecho (contracts/observability.md): `balance.events{outcome, reason}` com exatamente um desfecho por
 * evento (`reason=none` fora de `rejected`) e `balance.events.anomalies{type=conflicting_duplicate}`, que e adicional e nao
 * conta um segundo desfecho.
 */
@Component
class MicrometerProcessingMetrics(
    registry: MeterRegistry,
) : ProcessingMetrics {
    private val processed = outcome(registry, "processed")
    private val obsolete = outcome(registry, "obsolete")
    private val duplicate = outcome(registry, "duplicate")
    private val conflictingDuplicate =
        Counter
            .builder("balance.events.anomalies")
            .description("Eventos com a mesma chave de precedencia e conteudo divergente")
            .tag("type", "conflicting_duplicate")
            .register(registry)

    override fun applied() = processed.increment()

    override fun obsolete() = obsolete.increment()

    override fun duplicate(conflicting: Boolean) {
        duplicate.increment()
        if (conflicting) conflictingDuplicate.increment()
    }

    private fun outcome(
        registry: MeterRegistry,
        outcome: String,
    ): Counter =
        Counter
            .builder("balance.events")
            .description("Desfecho de cada evento consumido")
            .tag("outcome", outcome)
            .tag("reason", "none")
            .register(registry)
}
