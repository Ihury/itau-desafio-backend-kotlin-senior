package br.com.itau.challenge.balance.adapter.output.metrics

import br.com.itau.challenge.balance.domain.model.RejectionReason
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

    private fun rejected(reason: String): Double = registry.find("balance.events").tags("outcome", "rejected", "reason", reason).counter()?.count() ?: 0.0

    private fun dltFailures(): Double = registry.find("balance.dlt.publish.failures").counter()?.count() ?: 0.0

    @Test
    fun `rejected counts the outcome under the reason code and touches no other outcome`() {
        metrics.rejected(RejectionReason.INVALID_CURRENCY)

        assertEquals(1.0, rejected("invalid_currency"))
        assertEquals(0.0, rejected("malformed_payload"))
        assertEquals(0.0, events("processed"))
        assertEquals(0.0, events("obsolete"))
        assertEquals(0.0, events("duplicate"))
    }

    @Test
    fun `every reason of the catalog has its own rejected counter registered at zero`() {
        RejectionReason.entries.forEach { reason ->
            assertEquals(0.0, registry.find("balance.events").tags("outcome", "rejected", "reason", reason.code).counter()?.count(), reason.code)
        }
        RejectionReason.entries.forEach { metrics.rejected(it) }
        RejectionReason.entries.forEach { assertEquals(1.0, rejected(it.code), it.code) }
    }

    @Test
    fun `a dlt publication failure increments its own counter and is not an outcome`() {
        metrics.dltPublishFailed()
        metrics.dltPublishFailed()

        assertEquals(2.0, dltFailures())
        RejectionReason.entries.forEach { assertEquals(0.0, rejected(it.code)) }
    }
}
