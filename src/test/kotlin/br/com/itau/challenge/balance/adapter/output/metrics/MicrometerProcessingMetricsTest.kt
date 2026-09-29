package br.com.itau.challenge.balance.adapter.output.metrics

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class MicrometerProcessingMetricsTest {
    private val registry = SimpleMeterRegistry()
    private val metrics = MicrometerProcessingMetrics(registry)

    private fun events(outcome: String): Double = registry.find("balance.events").tags("outcome", outcome, "reason", "none").counter()?.count() ?: 0.0

    private fun anomalies(): Double = registry.find("balance.events.anomalies").tag("type", "conflicting_duplicate").counter()?.count() ?: 0.0

    @Test
    fun `applied increments only the processed outcome`() {
        metrics.applied()

        assertEquals(1.0, events("processed"))
        assertEquals(0.0, events("obsolete"))
        assertEquals(0.0, events("duplicate"))
        assertEquals(0.0, anomalies())
    }

    @Test
    fun `obsolete increments only the obsolete outcome`() {
        metrics.obsolete()

        assertEquals(1.0, events("obsolete"))
        assertEquals(0.0, events("processed"))
    }

    @Test
    fun `a plain duplicate increments only the duplicate outcome`() {
        metrics.duplicate(conflicting = false)

        assertEquals(1.0, events("duplicate"))
        assertEquals(0.0, anomalies())
    }

    @Test
    fun `a conflicting duplicate counts the outcome once as duplicate and adds the anomaly`() {
        metrics.duplicate(conflicting = true)

        assertEquals(1.0, events("duplicate"))
        assertEquals(1.0, anomalies())
        assertEquals(0.0, events("processed"))
        assertEquals(0.0, events("obsolete"))
    }

    @Test
    fun `counters accumulate across calls`() {
        repeat(3) { metrics.applied() }
        repeat(2) { metrics.obsolete() }
        metrics.duplicate(conflicting = false)
        metrics.duplicate(conflicting = true)

        assertEquals(3.0, events("processed"))
        assertEquals(2.0, events("obsolete"))
        assertEquals(2.0, events("duplicate"))
        assertEquals(1.0, anomalies())
    }
}
