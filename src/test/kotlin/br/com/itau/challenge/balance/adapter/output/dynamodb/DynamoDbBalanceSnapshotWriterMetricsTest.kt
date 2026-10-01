package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.transactionEvent
import org.junit.jupiter.api.Test
import software.amazon.awssdk.core.exception.ApiCallTimeoutException
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class DynamoDbBalanceSnapshotWriterMetricsTest : DynamoDbWriterFixture() {
    private fun writeTimer(result: String) = registry.find("balance.store.write.duration").tag("result", result).timer()

    @Test
    fun `write duration is timed as applied when the condition holds`() {
        stubUpdateSucceeds()

        writer.applyIfNewer(snapshot)

        assertEquals(1L, writeTimer("applied")?.count())
        assertEquals(0L, writeTimer("condition_failed")?.count())
        assertEquals(0L, writeTimer("error")?.count())
    }

    @Test
    fun `write duration is timed as condition failed for duplicates and obsolete events, which are not errors`() {
        stubConditionFailedWithCurrentItem(itemOf(transactionEvent()))
        writer.applyIfNewer(snapshot)
        stubConditionFailedWithCurrentItem(itemOf(transactionEvent(timestampMicros = 1751749453433999L)))
        writer.applyIfNewer(snapshot)

        assertEquals(2L, writeTimer("condition_failed")?.count())
        assertEquals(0L, writeTimer("error")?.count())
    }

    @Test
    fun `write duration is recorded as error when the sdk throws`() {
        stubUpdateFailsWith(ApiCallTimeoutException.builder().message("x").build())

        assertFailsWith<BalanceStoreUnavailableException> { writer.applyIfNewer(snapshot) }

        assertEquals(1L, writeTimer("error")?.count())
    }

    @Test
    fun `write duration is never tagged with account data`() {
        stubUpdateFailsWith(ApiCallTimeoutException.builder().message("x").build())

        assertFailsWith<BalanceStoreUnavailableException> { writer.applyIfNewer(snapshot) }

        val tagKeys = registry.find("balance.store.write.duration").timers().flatMap { timer -> timer.id.tags.map { it.key } }.toSet()
        assertEquals(setOf("result"), tagKeys)
    }

    @Test
    fun `write duration publishes a histogram with the documented service level objectives`() {
        stubUpdateSucceeds()
        writer.applyIfNewer(snapshot)

        val buckets = writeTimer("applied")!!.takeSnapshot().histogramCounts().map { it.bucket(TimeUnit.MILLISECONDS) }
        assertTrue(buckets.isNotEmpty(), "sem histograma")
        assertTrue(5.0 in buckets && 2000.0 in buckets, "buckets $buckets")
    }
}
