package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import br.com.itau.challenge.balance.port.output.IngestMetrics
import br.com.itau.challenge.balance.port.output.IngestOutcome
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.MDC
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

/**
 * Entrada Kafka: um registro por vez, com os bytes verbatim (`ByteArrayDeserializer` nunca lanca), convertidos pelo parser
 * estrito e entregues ao caso de uso. Nao recebe `Acknowledgment`: o commit do offset e do container (`AckMode=BATCH`, depois
 * que o listener retorna para todos os registros do poll), o que da entrega at-least-once. Qualquer excecao propaga ao error
 * handler do container; nada e engolido.
 *
 * O MDC leva `correlationId=<topic>-<partition>@<offset>` durante todo o processamento e `accountId`/`transactionId` somente
 * depois que o evento foi parseado; e sempre limpo em `finally`. Nunca registra payload, saldo nem titular.
 *
 * A duracao `balance.ingest.duration{outcome}` e registrada por mensagem em `finally`: `processed`, `obsolete` ou `duplicate`
 * conforme o caso de uso, `rejected` para [InvalidEventException] e `error` para qualquer outra falha.
 */
@Component
class TransactionEventListener(
    private val parser: TransactionEventParser,
    private val processTransactionEvent: ProcessTransactionEventUseCase,
    private val ingestMetrics: IngestMetrics,
) {
    @KafkaListener(id = LISTENER_ID, idIsGroup = false, topics = ["\${balance.events.topic}"])
    fun onMessage(record: ConsumerRecord<ByteArray?, ByteArray?>) {
        val startedNanos = System.nanoTime()
        var outcome = IngestOutcome.ERROR
        MDC.put(CORRELATION_ID, "${record.topic()}-${record.partition()}@${record.offset()}")
        try {
            val event = parser.parse(record.value() ?: throw InvalidEventException(RejectionReason.MALFORMED_PAYLOAD))
            MDC.put(ACCOUNT_ID, event.account.id.value)
            MDC.put(TRANSACTION_ID, event.transaction.id.value)
            outcome = outcomeOf(processTransactionEvent.process(event))
        } catch (invalid: InvalidEventException) {
            outcome = IngestOutcome.REJECTED
            throw invalid
        } finally {
            ingestMetrics.ingestDuration(outcome, System.nanoTime() - startedNanos)
            MDC.remove(TRANSACTION_ID)
            MDC.remove(ACCOUNT_ID)
            MDC.remove(CORRELATION_ID)
        }
    }

    private fun outcomeOf(result: ApplyResult): IngestOutcome =
        when (result) {
            is ApplyResult.Applied -> IngestOutcome.PROCESSED
            is ApplyResult.Obsolete -> IngestOutcome.OBSOLETE
            is ApplyResult.Duplicate -> IngestOutcome.DUPLICATE
        }

    companion object {
        /** Id do listener no registry (o grupo de consumo vem de `spring.kafka.consumer.group-id`). */
        const val LISTENER_ID = "transaction-event-listener"
        private const val CORRELATION_ID = "correlationId"
        private const val ACCOUNT_ID = "accountId"
        private const val TRANSACTION_ID = "transactionId"
    }
}
