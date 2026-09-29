package br.com.itau.challenge.balance.adapter.output.metrics

import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.port.output.ProcessingMetrics
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component

/**
 * Contadores de desfecho (contracts/observability.md): `balance.events{outcome, reason}` com exatamente um desfecho por
 * evento (`reason=none` fora de `rejected`) e `balance.events.anomalies{type=conflicting_duplicate}`, que e adicional e nao
 * conta um segundo desfecho. `rejected` tem um contador por motivo do catalogo (todos registrados em zero) e
 * `balance.dlt.publish.failures` conta as falhas de publicacao no DLT, que nao sao desfecho.
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

    private val rejected: Map<RejectionReason, Counter> =
        RejectionReason.entries.associateWith { reason ->
            Counter
                .builder("balance.events")
                .description("Desfecho de cada evento consumido")
                .tag("outcome", "rejected")
                .tag("reason", reason.code)
                .register(registry)
        }
    private val dltPublishFailures =
        Counter
            .builder("balance.dlt.publish.failures")
            .description("Falhas ao publicar no DLT (mensagem nao confirmada e reentregue)")
            .register(registry)

    override fun applied() = processed.increment()

    override fun obsolete() = obsolete.increment()

    override fun duplicate(conflicting: Boolean) {
        duplicate.increment()
        if (conflicting) conflictingDuplicate.increment()
    }

    override fun rejected(reason: RejectionReason) = rejected.getValue(reason).increment()

    override fun dltPublishFailed() = dltPublishFailures.increment()

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
