package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.BalanceStoreRejectedException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.testing.RecordingProcessingMetrics
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.mock
import org.mockito.Mockito.mockingDetails
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.listener.ContainerPausingBackOffHandler
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.listener.ListenerExecutionFailedException
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.scheduling.TaskScheduler
import org.springframework.util.backoff.BackOffExecution
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.max
import kotlin.math.min
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Backpressure da falha transitoria (FR-028, FR-029): `ExponentialBackOff` com jitter nativo e sem esgotar, pausa do container
 * durante a espera (o poll continua vivo) e `balance.consumer.backpressure{cause}`. O handler e chamado diretamente, sem broker.
 */
class BackpressureConfigTest {
    private val settings = BackOffProperties(initialMs = 500, maxMs = 30_000, jitterMs = 250)
    private val config = DeadLetterConfig()
    private val backpressureConfig = BackpressureConfig()

    // ----- o backoff ---------------------------------------------------------------------------------------------------------

    /**
     * Faixa do n-esimo intervalo (0-based) do `ExponentialBackOff` do Spring Framework 7: o jitter escala com o multiplicador
     * (`jitter x intervalo / inicial`), o piso e o intervalo inicial e o teto e o maximo.
     */
    private fun envelope(step: Int): LongRange {
        var base = settings.initialMs
        repeat(step) { base = min((base * 2.0).toLong(), settings.maxMs) }
        val scaledJitter = settings.jitterMs * (base / settings.initialMs)
        return max(base - scaledJitter, settings.initialMs)..min(base + scaledJitter, settings.maxMs)
    }

    @Test
    fun `the back off is exponential from 500 ms by two up to thirty seconds with 250 ms of jitter and no attempt or time limit`() {
        val backOff = config.transientBackOff(settings)

        assertEquals(500L, backOff.initialInterval)
        assertEquals(2.0, backOff.multiplier)
        assertEquals(30_000L, backOff.maxInterval)
        assertEquals(250L, backOff.jitter)
        assertEquals(Long.MAX_VALUE, backOff.maxAttempts)
        assertEquals(Long.MAX_VALUE, backOff.maxElapsedTime)
    }

    @Test
    fun `the back off never runs out and every wait stays inside its jitter envelope and under the ceiling`() {
        repeat(20) {
            val execution = config.transientBackOff(settings).start()

            (0 until 1000).forEach { step ->
                val wait = execution.nextBackOff()

                assertTrue(wait != BackOffExecution.STOP, "o backoff nao pode esgotar (passo ${step + 1})")
                val expected = envelope(step.coerceAtMost(20))
                assertTrue(wait in expected, "passo ${step + 1}: $wait fora de $expected")
                assertTrue(wait <= 30_000L, "nunca acima do teto de 30 s")
            }
        }
    }

    @Test
    fun `the waits grow across attempts on average, and the jitter really varies them`() {
        val runs = 400
        val samples = (0 until runs).map { config.transientBackOff(settings).start().let { execution -> (0 until 10).map { execution.nextBackOff() } } }
        val means = (0 until 10).map { step -> samples.map { it[step] }.average() }

        (0 until 6).forEach { step -> assertTrue(means[step] < means[step + 1], "espera media deve crescer no passo ${step + 1}: $means") }
        assertTrue(means[9] >= 15_000.0, "no teto a espera fica entre 15 s e 30 s: $means")
        val third = samples.map { it[2] }.toSet()
        assertTrue(third.size > 50, "o jitter precisa variar o intervalo (valores distintos: ${third.size})")
        assertTrue(third.any { it < 2000 } && third.any { it > 2000 }, "o jitter age para os dois lados do intervalo base")
    }

    @Test
    fun `without jitter the sequence is deterministic`() {
        val execution = config.transientBackOff(settings.copy(jitterMs = 0)).start()

        assertEquals(listOf(500L, 1000L, 2000L, 4000L, 8000L, 16000L, 30000L, 30000L), (0 until 8).map { execution.nextBackOff() })
    }

    @Test
    fun `the parameters come from the settings and the wait never reaches the poll interval`() {
        val custom = BackOffProperties(initialMs = 100, maxMs = 1_000, jitterMs = 50)
        val backOff = config.transientBackOff(custom)

        assertEquals(100L, backOff.initialInterval)
        assertEquals(1_000L, backOff.maxInterval)
        assertEquals(50L, backOff.jitter)
        val execution = backOff.start()
        assertTrue((0 until 200).all { execution.nextBackOff() in 100..1_000 })
    }

    @Test
    fun `the back off function still gives each class its own policy, so the default back off must be the transient one`() {
        assertTrue(config.backOffFor(BalanceStoreUnavailableException(StoreFailureCause.TIMEOUT), settings).start().nextBackOff() in 500..750)
        assertEquals(100L, config.backOffFor(IllegalStateException(), settings).start().nextBackOff())
        assertEquals(BackOffExecution.STOP, config.backOffFor(InvalidEventException(RejectionReason.INVALID_VALUE), settings).start().nextBackOff())
    }

    // ----- pausa do container e recoverer nunca acionado ---------------------------------------------------------------------

    private val dlt = mock(KafkaOperations::class.java)
    private val metrics = RecordingProcessingMetrics()
    private val scheduler = mock(TaskScheduler::class.java)

    @Suppress("UNCHECKED_CAST")
    private fun handler(): DefaultErrorHandler =
        config.deadLetterErrorHandler(
            dlt as KafkaOperations<ByteArray, ByteArray>,
            "transacoes-financeiras-processadas.DLT",
            Duration.ofSeconds(5),
            Clock.systemUTC(),
            metrics,
            settings,
            backpressureConfig.containerPausingBackOffHandler(scheduler),
        )

    private val record = ConsumerRecord<Any, Any>("transacoes-financeiras-processadas", 2, 41L, null, ByteArray(0))
    private val consumer = mock(Consumer::class.java)

    /** Container com o estado de pausa de verdade: `pause()` liga, `resume()` desliga e `isPauseRequested()` reflete. */
    private fun statefulContainer(): Pair<MessageListenerContainer, AtomicBoolean> {
        val paused = AtomicBoolean(false)
        val container = mock(MessageListenerContainer::class.java)
        doAnswer { paused.set(true) }.`when`(container).pause()
        doAnswer { paused.set(false) }.`when`(container).resume()
        doAnswer { paused.get() }.`when`(container).isPauseRequested
        return container to paused
    }

    private fun transientFailure(cause: StoreFailureCause = StoreFailureCause.UNAVAILABLE) =
        ListenerExecutionFailedException("Listener failed", BalanceStoreUnavailableException(cause))

    @Test
    fun `the error handler pauses the container for the back off instead of sleeping in the poll thread`() {
        val handler = handler()
        val (container, paused) = statefulContainer()
        val before = Instant.now()

        handler.handleOne(transientFailure(), record, consumer, container)

        verify(container, times(1)).pause()
        assertTrue(paused.get(), "container pausado durante a espera")
        val resumeAt = ArgumentCaptor.forClass(Instant::class.java)
        val resume = ArgumentCaptor.forClass(Runnable::class.java)
        verify(scheduler).schedule(resume.capture(), resumeAt.capture())
        // primeiro intervalo: 500 ms a 750 ms (jitter escalado); a retomada e agendada, nao dormida
        val waited = Duration.between(before, resumeAt.value)
        assertTrue(waited >= Duration.ofMillis(400) && waited <= Duration.ofMillis(1_100), "retomada agendada em ~500..750 ms: $waited")

        resume.value.run()

        verify(container).resume()
        assertFalse(paused.get(), "a retomada solta a pausa")
    }

    @Test
    fun `the pause is longer at each redelivery until the ceiling`() {
        val handler = handler()
        val (container, _) = statefulContainer()
        val delays = mutableListOf<Duration>()
        val resumeAt = ArgumentCaptor.forClass(Instant::class.java)
        val resume = ArgumentCaptor.forClass(Runnable::class.java)

        repeat(12) {
            val before = Instant.now()
            handler.handleOne(transientFailure(), record, consumer, container)
            verify(scheduler, times(it + 1)).schedule(resume.capture(), resumeAt.capture())
            delays += Duration.between(before, resumeAt.value)
            resume.value.run()
        }

        assertTrue(delays.last() <= Duration.ofMillis(30_500), "teto de 30 s: ${delays.last()}")
        assertTrue(delays.last() >= Duration.ofSeconds(14), "no teto a pausa fica entre 15 s e 30 s: ${delays.last()}")
        assertTrue(delays[8] > delays[0], "pausa cresce: $delays")
    }

    @Test
    fun `fifty transient failures never reach the recoverer and never touch the dlt`() {
        val handler = handler()
        val (container, _) = statefulContainer()

        repeat(50) {
            val recovered = handler.handleOne(transientFailure(StoreFailureCause.entries[it % 3]), record, consumer, container)
            assertFalse(recovered, "descartada na tentativa ${it + 1}")
            resumeIfPaused(container)
        }

        assertTrue(mockingDetails(dlt).invocations.none { it.method.name == "send" }, "nada pode ser publicado no DLT")
        assertEquals(emptyList(), metrics.outcomes)
        assertEquals(0, metrics.dltPublishFailures)
        verify(consumer, never()).commitSync()
    }

    private fun resumeIfPaused(container: MessageListenerContainer) {
        if (container.isPauseRequested) container.resume()
    }

    @Test
    fun `the container pausing handler is the one the configuration builds`() {
        assertTrue(backpressureConfig.containerPausingBackOffHandler(scheduler) is ContainerPausingBackOffHandler)
    }

    // ----- metrica de backpressure ---------------------------------------------------------------------------------------------

    @Test
    fun `every failed delivery of a transient failure counts backpressure with the cause of the store failure`() {
        val handler = handler()
        val (container, _) = statefulContainer()

        StoreFailureCause.entries.forEach { cause ->
            handler.handleOne(transientFailure(cause), record, consumer, container)
            resumeIfPaused(container)
        }
        handler.handleOne(transientFailure(StoreFailureCause.THROTTLED), record, consumer, container)

        assertEquals(
            listOf(StoreFailureCause.THROTTLED, StoreFailureCause.UNAVAILABLE, StoreFailureCause.TIMEOUT, StoreFailureCause.MISCONFIGURED, StoreFailureCause.THROTTLED),
            metrics.backpressureCauses,
        )
        assertEquals(emptyList(), metrics.outcomes, "backpressure nao e desfecho")
    }

    @Test
    fun `an unwrapped transient failure counts too, and other failures never count backpressure`() {
        val handler = handler()
        val (container, _) = statefulContainer()

        handler.handleOne(BalanceStoreUnavailableException(StoreFailureCause.TIMEOUT), record, consumer, container)
        resumeIfPaused(container)
        handler.handleOne(ListenerExecutionFailedException("x", IllegalStateException()), record, consumer, container)
        handler.handleOne(ListenerExecutionFailedException("x", BalanceStoreRejectedException()), record, consumer, container)

        assertEquals(listOf(StoreFailureCause.TIMEOUT), metrics.backpressureCauses)
    }
}
