package br.com.itau.challenge.balance.adapter.output.metrics

import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.port.output.ProcessingMetrics
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component

/**
 * Um contador `balance.events{outcome, reason}` por desfecho (`reason=none` fora de `rejected`; `rejected` tem um por motivo
 * do catalogo, todos registrados em zero). `balance.events.anomalies{type=conflicting_duplicate}` e adicional: nao conta um
 * segundo desfecho. `balance.dlt.publish.failures` e `balance.consumer.backpressure{cause}` nao sao desfechos.
 */
@Component
class MicrometerProcessingMetrics(
    registry: MeterRegistry,
) : ProcessingMetrics {
    private val processedCounter = outcomeCounter(registry, "processed")
    private val obsoleteCounter = outcomeCounter(registry, "obsolete")
    private val duplicateCounter = outcomeCounter(registry, "duplicate")
    private val conflictingDuplicate =
        Counter
            .builder("balance.events.anomalies")
            .description("Eventos com a mesma chave de precedencia e conteudo divergente")
            .tag("type", "conflicting_duplicate")
            .register(registry)

    private val rejectedCounters: Map<RejectionReason, Counter> =
        RejectionReason.entries.associateWith { reason ->
            Counter
                .builder("balance.events")
                .description("Desfecho de cada evento consumido")
                .tag("outcome", "rejected")
                .tag("reason", reason.code)
                .register(registry)
        }
    private val dltPublishFailuresCounter =
        Counter
            .builder("balance.dlt.publish.failures")
            .description("Falhas ao publicar no DLT (mensagem nao confirmada e reentregue)")
            .register(registry)

    private val backpressureCounters: Map<StoreFailureCause, Counter> =
        StoreFailureCause.entries.associateWith { cause ->
            Counter
                .builder("balance.consumer.backpressure")
                .description("Pausas do consumer por falha transitoria do armazenamento")
                .tag("cause", cause.name.lowercase())
                .register(registry)
        }

    override fun processed() = processedCounter.increment()

    override fun obsolete() = obsoleteCounter.increment()

    override fun duplicate(conflicting: Boolean) {
        duplicateCounter.increment()
        if (conflicting) conflictingDuplicate.increment()
    }

    override fun rejected(reason: RejectionReason) = rejectedCounters.getValue(reason).increment()

    override fun dltPublishFailed() = dltPublishFailuresCounter.increment()

    override fun backpressure(cause: StoreFailureCause) = backpressureCounters.getValue(cause).increment()

    private fun outcomeCounter(
        registry: MeterRegistry,
        label: String,
    ): Counter =
        Counter
            .builder("balance.events")
            .description("Desfecho de cada evento consumido")
            .tag("outcome", label)
            .tag("reason", "none")
            .register(registry)
}
