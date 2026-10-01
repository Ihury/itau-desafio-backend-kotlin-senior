package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.adapter.input.MdcKeys
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import br.com.itau.challenge.balance.port.output.IngestMetrics
import br.com.itau.challenge.balance.port.output.IngestOutcome
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

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
        try {
            outcome = withMdc(MdcKeys.CORRELATION_ID to record.coordinates()) { ingest(record) }
        } catch (invalid: InvalidEventException) {
            outcome = IngestOutcome.REJECTED
            throw invalid
        } finally {
            ingestMetrics.ingestDuration(outcome, System.nanoTime() - startedNanos)
        }
    }

    private fun ingest(record: ConsumerRecord<ByteArray?, ByteArray?>): IngestOutcome {
        val event = parser.parse(record.value())
        return withMdc(MdcKeys.ACCOUNT_ID to event.account.id.value, MdcKeys.TRANSACTION_ID to event.transaction.id.value) {
            outcomeOf(processTransactionEvent.process(event))
        }
    }

    private fun outcomeOf(result: ApplyResult): IngestOutcome =
        when (result) {
            is ApplyResult.Applied -> IngestOutcome.PROCESSED
            is ApplyResult.Obsolete -> IngestOutcome.OBSOLETE
            is ApplyResult.Duplicate -> IngestOutcome.DUPLICATE
        }

    companion object {
        const val LISTENER_ID = "transaction-event-listener"
    }
}
