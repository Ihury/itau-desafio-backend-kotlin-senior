package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.testing.NO_JITTER_BACK_OFF
import br.com.itau.challenge.balance.testing.RecordingBackOffHandler
import br.com.itau.challenge.balance.testing.RecordingProcessingMetrics
import br.com.itau.challenge.balance.testing.aRecord
import br.com.itau.challenge.balance.testing.isRedeliverySignal
import br.com.itau.challenge.balance.testing.listenerFailed
import br.com.itau.challenge.balance.testing.mockKafkaOperations
import br.com.itau.challenge.balance.testing.newDeadLetterErrorHandler
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyMap
import org.mockito.Mockito.mock
import org.mockito.Mockito.mockingDetails
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.springframework.core.NestedRuntimeException
import org.springframework.kafka.listener.BackOffHandler
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.util.backoff.BackOffExecution
import java.time.Clock
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class TransientFailureHandlingTest {
    private val dlt = mockKafkaOperations()
    private val metrics = RecordingProcessingMetrics()

    private fun errorHandlerWith(backOffHandler: BackOffHandler) =
        newDeadLetterErrorHandler(dlt, metrics, backOffHandler, waitForSendResultTimeout = Duration.ofSeconds(5), clock = Clock.systemUTC())

    private val record = aRecord(ByteArray(0), partition = 2)
    private val partition = TopicPartition(record.topic(), record.partition())

    private fun assertRedelivery(block: () -> Unit) {
        val signal = assertFailsWith<NestedRuntimeException> { block() }
        assertTrue(isRedeliverySignal(signal), "esperado o sinal de reentrega, veio ${signal.javaClass.simpleName}")
    }

    private val transientFailureFactories: Map<String, () -> Exception> =
        StoreFailureCause.entries.flatMap { cause ->
            listOf(
                "store unavailable ($cause)" to { listenerFailed(BalanceStoreUnavailableException(cause)) },
                "store unavailable ($cause), not wrapped" to { BalanceStoreUnavailableException(cause) as Exception },
            )
        }.toMap()

    @Test
    fun `the back off never runs out, grows to the ceiling and never waits past the poll interval`() {
        val execution = FailureBackOffs.transientFailure(NO_JITTER_BACK_OFF).start()
        val waits = (1..1000).map { execution.nextBackOff() }

        assertTrue(waits.none { it == BackOffExecution.STOP }, "o backoff nao pode esgotar")
        assertEquals(500L, waits.first())
        assertEquals(waits.sorted(), waits, "crescente (jitter zerado neste teste)")
        assertEquals(30_000L, waits.max(), "teto de 30 s")
        assertEquals(30_000L, waits.last())
        assertTrue(waits.all { it < MAX_POLL_INTERVAL_MS }, "nenhuma espera pode chegar ao max.poll.interval.ms")
        assertEquals(listOf(500L, 1000L, 2000L, 4000L, 8000L, 16000L, 30000L), waits.take(7))
    }

    @Test
    fun `the back off has no attempt or elapsed time limit`() {
        val backOff = FailureBackOffs.transientFailure(NO_JITTER_BACK_OFF)

        assertEquals(500L, backOff.initialInterval)
        assertEquals(2.0, backOff.multiplier)
        assertEquals(30_000L, backOff.maxInterval)
        assertEquals(Long.MAX_VALUE, backOff.maxAttempts)
        assertEquals(Long.MAX_VALUE, backOff.maxElapsedTime)
    }

    @Test
    fun `no failure is ever recovered, however many times the same record fails`() {
        transientFailureFactories.forEach { (label, failure) ->
            val backOffHandler = RecordingBackOffHandler()
            val handler = errorHandlerWith(backOffHandler)
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
        transientFailureFactories.forEach { (_, failure) ->
            val handler = errorHandlerWith(RecordingBackOffHandler())
            val consumer = mock(Consumer::class.java)
            val container = mock(MessageListenerContainer::class.java)
            val attempts = 50

            repeat(attempts) { assertRedelivery { handler.handleRemaining(failure(), listOf(record), consumer, container) } }

            verify(consumer, times(attempts)).seek(partition, record.offset())
            verify(consumer, never()).commitSync()
            verify(consumer, never()).commitSync(anyMap())
            verify(consumer, never()).commitAsync()
        }
    }

    @Test
    fun `the wait grows across redeliveries of the same record up to the ceiling for every kind of failure`() {
        transientFailureFactories.forEach { (label, failure) ->
            val backOffHandler = RecordingBackOffHandler()
            val handler = errorHandlerWith(backOffHandler)
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
        transientFailureFactories.forEach { (label, failure) ->
            val handler = errorHandlerWith(RecordingBackOffHandler())
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
            (1..20).firstOrNull { standard.handleOne(listenerFailed(IllegalStateException("x")), record, consumer, container) }

        assertTrue(recoveredAt != null && recoveredAt <= 10, "o padrao descarta apos ~10 tentativas: $recoveredAt")
    }

    private companion object {
        const val MAX_POLL_INTERVAL_MS = 300_000L
    }
}
