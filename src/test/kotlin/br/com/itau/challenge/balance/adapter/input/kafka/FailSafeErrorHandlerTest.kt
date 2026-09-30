package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyMap
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.mockingDetails
import br.com.itau.challenge.balance.testing.RecordingProcessingMetrics
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.listener.BackOffHandler
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.listener.ListenerExecutionFailedException
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.core.NestedRuntimeException
import org.springframework.util.backoff.BackOffExecution
import java.time.Clock
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Prova que a falha TRANSITORIA nunca descarta mensagem (Constitution III, FR-017): com o armazenamento indisponivel o error
 * handler nunca confirma o offset nem aciona o recoverer do DLT (a mensagem valida fica no broker), e a espera entre
 * reentregas cresce ate um teto e nunca se esgota. A partir da US4 as falhas permanentes e nao classificadas vao ao DLT
 * (`DeadLetterConfigTest`); aqui o `KafkaOperations` do DLT e um mock que NAO PODE ser usado. O handler e chamado
 * diretamente, sem broker.
 */
class FailSafeErrorHandlerTest {
    /** `BackOffHandler` que so registra o intervalo pedido, para nao dormir de verdade em mil iteracoes. */
    private class RecordingBackOffHandler : BackOffHandler {
        val intervals = mutableListOf<Long>()

        override fun onNextBackOff(
            container: MessageListenerContainer?,
            exception: Exception?,
            nextBackOff: Long,
        ) {
            intervals += nextBackOff
        }

        override fun onNextBackOff(
            container: MessageListenerContainer,
            partition: TopicPartition,
            nextBackOff: Long,
        ) {
            intervals += nextBackOff
        }
    }

    @Suppress("UNCHECKED_CAST")
    private val dlt = mock(KafkaOperations::class.java) as KafkaOperations<ByteArray, ByteArray>
    private val metrics = RecordingProcessingMetrics()
    private val config = DeadLetterConfig()

    /** Sem jitter, para as esperas serem exatas (o jitter e coberto por `BackpressureConfigTest`). */
    private val noJitter = BackOffProperties(initialMs = 500, maxMs = 30_000, jitterMs = 0)

    private fun failSafeErrorHandler(backOffHandler: BackOffHandler) =
        config.deadLetterErrorHandler(dlt, "transacoes-financeiras-processadas.DLT", Duration.ofSeconds(5), Clock.systemUTC(), metrics, noJitter, backOffHandler)
    private val record = ConsumerRecord<Any, Any>("transacoes-financeiras-processadas", 2, 41L, null, ByteArray(0))
    private val partition = TopicPartition(record.topic(), record.partition())

    /** `RecordInRetryException` (package-private no Spring Kafka) e o sinal de que o registro sera reentregue, nao pulado. */
    private fun assertRedelivery(block: () -> Unit) {
        val signal = assertFailsWith<NestedRuntimeException> { block() }
        assertEquals("RecordInRetryException", signal.javaClass.simpleName)
    }

    private fun listenerFailure(cause: Exception) = ListenerExecutionFailedException("Listener failed", cause)

    /** Falhas transitorias do armazenamento, com cada causa, embrulhadas como o container as entrega e sem embrulho. */
    private val failures: Map<String, () -> Exception> =
        StoreFailureCause.entries.flatMap { cause ->
            listOf(
                "store unavailable ($cause)" to { listenerFailure(BalanceStoreUnavailableException(cause)) },
                "store unavailable ($cause), not wrapped" to { BalanceStoreUnavailableException(cause) as Exception },
            )
        }.toMap()

    @Test
    fun `the back off never runs out, grows to the ceiling and never waits past the poll interval`() {
        val execution = config.transientBackOff(noJitter).start()
        val waits = (1..1000).map { execution.nextBackOff() }

        assertTrue(waits.none { it == BackOffExecution.STOP }, "o backoff nao pode esgotar")
        assertEquals(500L, waits.first())
        assertEquals(waits.sorted(), waits, "crescente (jitter zerado neste teste)")
        assertEquals(30_000L, waits.max(), "teto de 30 s")
        assertEquals(30_000L, waits.last())
        assertTrue(waits.all { it < 300_000L }, "nenhuma espera pode chegar ao max.poll.interval.ms")
        assertEquals(listOf(500L, 1000L, 2000L, 4000L, 8000L, 16000L, 30000L), waits.take(7))
    }

    @Test
    fun `the back off has no attempt or elapsed time limit`() {
        val backOff = config.transientBackOff(noJitter)

        assertEquals(500L, backOff.initialInterval)
        assertEquals(2.0, backOff.multiplier)
        assertEquals(30_000L, backOff.maxInterval)
        assertEquals(Long.MAX_VALUE, backOff.maxAttempts)
        assertEquals(Long.MAX_VALUE, backOff.maxElapsedTime)
    }

    @Test
    fun `no failure is ever recovered, however many times the same record fails`() {
        failures.forEach { (label, failure) ->
            val backOffHandler = RecordingBackOffHandler()
            val handler = failSafeErrorHandler(backOffHandler)
            val consumer = mock(Consumer::class.java)
            val container = mock(MessageListenerContainer::class.java)

            repeat(1000) { attempt ->
                val recovered = handler.handleOne(failure(), record, consumer, container)

                assertFalse(recovered, "$label descartada na tentativa ${attempt + 1}")
            }
            assertTrue(backOffHandler.intervals.none { it < 0 }, "$label: backoff esgotado")
            assertTrue(backOffHandler.intervals.all { it in 1..30_000 }, "$label: espera fora da faixa (0, 30 s]")
        }
    }

    @Test
    fun `the record stays unconfirmed and is redelivered after every failure`() {
        failures.forEach { (label, failure) ->
            val handler = failSafeErrorHandler(RecordingBackOffHandler())
            val consumer = mock(Consumer::class.java)
            val container = mock(MessageListenerContainer::class.java)
            val attempts = 50

            // RecordInRetryException e o sinal do Spring Kafka de que o registro sera reentregue (nao foi pulado)
            repeat(attempts) { assertRedelivery { handler.handleRemaining(failure(), listOf(record), consumer, container) } }

            // o registro e reposicionado para reentrega a cada falha e nenhum offset e confirmado
            verify(consumer, times(attempts)).seek(partition, record.offset())
            verify(consumer, never()).commitSync()
            verify(consumer, never()).commitSync(anyMap())
            verify(consumer, never()).commitAsync()
            assertTrue(label.isNotEmpty())
        }
    }

    @Test
    fun `the wait grows across redeliveries of the same record up to the ceiling for every kind of failure`() {
        failures.forEach { (label, failure) ->
            val backOffHandler = RecordingBackOffHandler()
            val handler = failSafeErrorHandler(backOffHandler)
            val consumer = mock(Consumer::class.java)
            val container = mock(MessageListenerContainer::class.java)

            repeat(20) { assertRedelivery { handler.handleRemaining(failure(), listOf(record), consumer, container) } }

            assertEquals(listOf(500L, 1000L, 2000L, 4000L, 8000L, 16000L), backOffHandler.intervals.take(6), label)
            assertEquals(30_000L, backOffHandler.intervals.last(), label)
            assertEquals(backOffHandler.intervals.sorted(), backOffHandler.intervals, label)
        }
    }

    @Test
    fun `the dlt is never touched and nothing is counted, however long the store stays down`() {
        failures.forEach { (label, failure) ->
            val handler = failSafeErrorHandler(RecordingBackOffHandler())
            val consumer = mock(Consumer::class.java)
            val container = mock(MessageListenerContainer::class.java)

            repeat(200) { assertFalse(handler.handleOne(failure(), record, consumer, container), label) }
        }

        assertTrue(mockingDetails(dlt).invocations.none { it.method.name == "send" }, "nada pode ser publicado no DLT")
        assertEquals(emptyList(), metrics.outcomes)
        assertEquals(0, metrics.dltPublishFailures)
    }

    @Test
    fun `the spring kafka default handler is exactly what this test forbids, so the test is not vacuous`() {
        val standard = DefaultErrorHandler()
        val consumer = mock(Consumer::class.java)
        val container = mock(MessageListenerContainer::class.java)

        val recoveredAt =
            (1..20).firstOrNull { standard.handleOne(listenerFailure(IllegalStateException("x")), record, consumer, container) }

        assertTrue(recoveredAt != null && recoveredAt <= 10, "o padrao descarta apos ~10 tentativas: $recoveredAt")
    }
}
