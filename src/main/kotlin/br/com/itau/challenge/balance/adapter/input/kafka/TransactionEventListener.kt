package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.MDC
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * Entrada Kafka: um registro por vez, com os bytes verbatim (`ByteArrayDeserializer` nunca lanca), convertidos pelo parser
 * estrito e entregues ao caso de uso. Nao recebe `Acknowledgment`: o commit do offset e do container (`AckMode=BATCH`, depois
 * que o listener retorna para todos os registros do poll), o que da entrega at-least-once (Constitution III). Qualquer excecao
 * propaga ao error handler do container; nada e engolido.
 *
 * O MDC leva `correlationId=<topic>-<partition>@<offset>` durante todo o processamento e `accountId`/`transactionId` somente
 * depois que o evento foi parseado; e sempre limpo em `finally`. Nunca registra payload, saldo nem titular.
 *
 * O timer `balance.ingest.duration{outcome}` (histograma, SLO de 5 ms a 2,5 s; parse + validacao + escrita) e registrado por
 * mensagem em `finally`: `processed`, `obsolete` ou `duplicate` conforme o caso de uso, `rejected` para [InvalidEventException]
 * e `error` para qualquer outra falha (a excecao sempre propaga ao error handler).
 */
@Component
class TransactionEventListener(
    private val parser: TransactionEventParser,
    private val processTransactionEvent: ProcessTransactionEventUseCase,
    meterRegistry: MeterRegistry,
) {
    private val durations: Map<String, Timer> = OUTCOMES.associateWith { outcome -> ingestTimer(meterRegistry, outcome) }

    @KafkaListener(id = LISTENER_ID, idIsGroup = false, topics = ["\${balance.events.topic}"])
    fun onMessage(record: ConsumerRecord<ByteArray?, ByteArray?>) {
        val started = System.nanoTime()
        var outcome = ERROR
        MDC.put(CORRELATION_ID, "${record.topic()}-${record.partition()}@${record.offset()}")
        try {
            val event = parser.parse(record.value() ?: throw InvalidEventException(RejectionReason.MALFORMED_PAYLOAD))
            MDC.put(ACCOUNT_ID, event.account.id.value)
            MDC.put(TRANSACTION_ID, event.transaction.id.value)
            outcome = outcomeOf(processTransactionEvent.process(event))
        } catch (invalid: InvalidEventException) {
            outcome = REJECTED
            throw invalid
        } finally {
            durations.getValue(outcome).record(System.nanoTime() - started, TimeUnit.NANOSECONDS)
            MDC.remove(TRANSACTION_ID)
            MDC.remove(ACCOUNT_ID)
            MDC.remove(CORRELATION_ID)
        }
    }

    private fun outcomeOf(result: ApplyResult): String =
        when (result) {
            is ApplyResult.Applied -> PROCESSED
            is ApplyResult.Obsolete -> OBSOLETE
            is ApplyResult.Duplicate -> DUPLICATE
        }

    companion object {
        /** Id do listener no registry (o grupo de consumo vem de `spring.kafka.consumer.group-id`). */
        const val LISTENER_ID = "transaction-event-listener"
        private const val CORRELATION_ID = "correlationId"
        private const val ACCOUNT_ID = "accountId"
        private const val TRANSACTION_ID = "transactionId"
        private const val PROCESSED = "processed"
        private const val OBSOLETE = "obsolete"
        private const val DUPLICATE = "duplicate"
        private const val REJECTED = "rejected"
        private const val ERROR = "error"
        private val OUTCOMES = listOf(PROCESSED, OBSOLETE, DUPLICATE, REJECTED, ERROR)

        /** Objetivos do histograma: de 5 ms a 2,5 s (contracts/observability.md). */
        private val OBJECTIVES = listOf(5L, 10L, 25L, 50L, 100L, 250L, 500L, 1_000L, 2_500L).map(Duration::ofMillis).toTypedArray()

        /** As cinco series nascem em zero, para as consultas do Prometheus enxergarem a serie antes da primeira mensagem. */
        private fun ingestTimer(
            registry: MeterRegistry,
            outcome: String,
        ): Timer =
            Timer
                .builder("balance.ingest.duration")
                .description("Parse, validacao e escrita de cada mensagem consumida")
                .tag("outcome", outcome)
                .serviceLevelObjectives(*OBJECTIVES)
                .register(registry)
    }
}
