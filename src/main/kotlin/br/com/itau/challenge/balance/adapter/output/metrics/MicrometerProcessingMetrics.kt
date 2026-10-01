package br.com.itau.challenge.balance.adapter.output.metrics

import br.com.itau.challenge.balance.adapter.output.ResultTimers
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.port.output.ConsumerFailureMetrics
import br.com.itau.challenge.balance.port.output.IngestMetrics
import br.com.itau.challenge.balance.port.output.IngestOutcome
import br.com.itau.challenge.balance.port.output.OutcomeMetrics
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component

@Component
class MicrometerProcessingMetrics(
    registry: MeterRegistry,
) : OutcomeMetrics,
    ConsumerFailureMetrics,
    IngestMetrics {
    private val processedCounter = eventCounter(registry, "processed", NO_REASON)
    private val obsoleteCounter = eventCounter(registry, "obsolete", NO_REASON)
    private val duplicateCounter = eventCounter(registry, "duplicate", NO_REASON)
    private val conflictingDuplicate =
        Counter
            .builder("balance.events.anomalies")
            .description("Eventos com a mesma chave de precedencia e conteudo divergente")
            .tag("type", "conflicting_duplicate")
            .register(registry)

    private val rejectedCounters: Map<RejectionReason, Counter> =
        RejectionReason.entries.associateWith { reason -> eventCounter(registry, "rejected", reason.code) }
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

    private val ingestTimers =
        ResultTimers(
            registry = registry,
            name = "balance.ingest.duration",
            description = "Parse, validacao e escrita de cada mensagem consumida",
            tagName = "outcome",
            results = IngestOutcome.entries,
            objectivesMillis = INGEST_OBJECTIVES_MILLIS,
        )

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
    ) = ingestTimers.recordElapsed(outcome, nanos)

    private fun eventCounter(
        registry: MeterRegistry,
        outcome: String,
        reason: String,
    ): Counter =
        Counter
            .builder("balance.events")
            .description("Desfecho de cada evento consumido")
            .tag("outcome", outcome)
            .tag("reason", reason)
            .register(registry)

    private companion object {
        const val NO_REASON = "none"
        val INGEST_OBJECTIVES_MILLIS = listOf(5L, 10L, 25L, 50L, 100L, 250L, 500L, 1_000L, 2_500L)
    }
}
