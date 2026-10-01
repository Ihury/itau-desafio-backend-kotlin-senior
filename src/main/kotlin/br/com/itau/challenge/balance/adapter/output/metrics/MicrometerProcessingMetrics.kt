package br.com.itau.challenge.balance.adapter.output.metrics

import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.port.output.ConsumerFailureMetrics
import br.com.itau.challenge.balance.port.output.IngestMetrics
import br.com.itau.challenge.balance.port.output.IngestOutcome
import br.com.itau.challenge.balance.port.output.OutcomeMetrics
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * Um contador `balance.events{outcome, reason}` por desfecho (`reason=none` fora de `rejected`; `rejected` tem um por motivo
 * do catalogo, todos registrados em zero). `balance.events.anomalies{type=conflicting_duplicate}` e adicional: nao conta um
 * segundo desfecho. `balance.dlt.publish.failures` e `balance.consumer.backpressure{cause}` nao sao desfechos.
 * `balance.ingest.duration{outcome}` nasce com as cinco series em zero.
 */
@Component
class MicrometerProcessingMetrics(
    registry: MeterRegistry,
) : OutcomeMetrics,
    ConsumerFailureMetrics,
    IngestMetrics {
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

    private val ingestTimers: Map<IngestOutcome, Timer> =
        IngestOutcome.entries.associateWith { outcome ->
            Timer
                .builder("balance.ingest.duration")
                .description("Parse, validacao e escrita de cada mensagem consumida")
                .tag("outcome", outcome.name.lowercase())
                .serviceLevelObjectives(*INGEST_OBJECTIVES)
                .register(registry)
        }

    override fun record(result: ApplyResult) {
        when (result) {
            is ApplyResult.Applied -> processedCounter.increment()
            is ApplyResult.Obsolete -> obsoleteCounter.increment()
            is ApplyResult.Duplicate -> {
                duplicateCounter.increment()
                if (result.conflicting) conflictingDuplicate.increment()
            }
        }
    }

    override fun rejected(reason: RejectionReason) = rejectedCounters.getValue(reason).increment()

    override fun dltPublishFailed() = dltPublishFailuresCounter.increment()

    override fun backpressure(cause: StoreFailureCause) = backpressureCounters.getValue(cause).increment()

    override fun ingestDuration(
        outcome: IngestOutcome,
        nanos: Long,
    ) = ingestTimers.getValue(outcome).record(nanos, TimeUnit.NANOSECONDS)

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

    private companion object {
        val INGEST_OBJECTIVES = listOf(5L, 10L, 25L, 50L, 100L, 250L, 500L, 1_000L, 2_500L).map(Duration::ofMillis).toTypedArray()
    }
}
