package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.testing.DEAD_LETTER_TOPIC
import br.com.itau.challenge.balance.testing.DeadLetterHarness
import br.com.itau.challenge.balance.testing.ORIGINAL_KEY
import br.com.itau.challenge.balance.testing.ORIGINAL_VALUE
import br.com.itau.challenge.balance.testing.TRANSACTIONS_TOPIC
import br.com.itau.challenge.balance.testing.aRecord
import br.com.itau.challenge.balance.testing.headerText
import br.com.itau.challenge.balance.testing.listenerFailed
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mockingDetails
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DeadLetterRoutingTest {
    private val dlt = DeadLetterHarness()

    @Test
    fun `an invalid event goes to the dlt on the first failure without redelivery`() {
        assertTrue(dlt.deliver(InvalidEventException(RejectionReason.INVALID_CURRENCY, "transaction.currency")))

        assertEquals(1, dlt.publications.size)
        assertEquals(emptyList(), dlt.backOffs.intervals, "sem espera nem reentrega")
        assertEquals(listOf("rejected(invalid_currency)"), dlt.metrics.outcomes)
    }

    @Test
    fun `the destination is the dlt topic with the partition left to the partitioner`() {
        dlt.deliver(InvalidEventException(RejectionReason.MISSING_FIELD, "account.id"))

        val outbound = dlt.singleDltRecord()
        assertEquals(DEAD_LETTER_TOPIC, outbound.topic())
        assertNull(outbound.partition(), "partition -1 (o padrao 'mesma particao' falharia com 12 -> 3 particoes)")
    }

    @Test
    fun `publishing to the dlt never asks the broker for the partitions of the dlt topic`() {
        dlt.deliver(InvalidEventException(RejectionReason.MISSING_FIELD, "account.id"))

        assertTrue(mockingDetails(dlt.consumer).invocations.none { it.method.name == "partitionsFor" })
    }

    @Test
    fun `key and value reach the dlt as the original bytes, binary included`() {
        dlt.deliver(InvalidEventException(RejectionReason.MALFORMED_PAYLOAD))

        val outbound = dlt.singleDltRecord()
        assertContentEquals(ORIGINAL_VALUE, outbound.value())
        assertContentEquals(ORIGINAL_KEY, outbound.key())
    }

    @Test
    fun `a record without key is published without key`() {
        val keyless = aRecord(ORIGINAL_VALUE, partition = 3, offset = 9L)

        dlt.deliver(keyless, InvalidEventException(RejectionReason.MALFORMED_PAYLOAD))

        assertNull(dlt.singleDltRecord().key())
        assertContentEquals(ORIGINAL_VALUE, dlt.singleDltRecord().value())
    }

    @Test
    fun `the rejection headers and the original coordinates are present and no exception header leaks`() {
        dlt.deliver(InvalidEventException(RejectionReason.INVALID_TIMESTAMP, "account.created_at"))

        val outbound = dlt.singleDltRecord()
        assertEquals("invalid_timestamp", outbound.headerText("x-rejection-reason"))
        assertEquals("account.created_at", outbound.headerText("x-rejection-detail"))
        assertEquals("2026-06-01T12:00:00Z", outbound.headerText("x-rejected-at"))
        assertNotNull(outbound.headers().lastHeader("kafka_dlt-original-topic"))
        assertNotNull(outbound.headers().lastHeader("kafka_dlt-original-partition"))
        assertNotNull(outbound.headers().lastHeader("kafka_dlt-original-offset"))
        assertNotNull(outbound.headers().lastHeader("kafka_dlt-original-timestamp"))
        assertNotNull(outbound.headers().lastHeader("kafka_dlt-original-timestamp-type"))
        assertEquals(TRANSACTIONS_TOPIC, outbound.headerText("kafka_dlt-original-topic"))
        val keys = outbound.headers().map { it.key() }
        assertTrue(keys.none { it.startsWith("kafka_dlt-exception") }, "headers de excecao vazam: $keys")
        assertTrue(keys.none { it.contains("stacktrace", ignoreCase = true) }, "pilha vaza: $keys")
    }

    @Test
    fun `headers never carry the exception text or values, only codes and paths`() {
        dlt.deliver(InvalidEventException(RejectionReason.INVALID_VALUE, "transaction.amount"))

        val everything = dlt.singleDltRecord().headers().joinToString(" ") { "${it.key()}=${it.value().toString(Charsets.UTF_8)}" }
        assertFalse("Listener failed" in everything)
        assertFalse("ListenerExecutionFailedException" in everything)
        assertFalse("InvalidEventException" in everything)
    }

    @Test
    fun `the detail header is absent for a payload rejection without field path`() {
        dlt.deliver(InvalidEventException(RejectionReason.MALFORMED_PAYLOAD))

        assertNull(dlt.singleDltRecord().headers().lastHeader("x-rejection-detail"))
    }

    @Test
    fun `record level handling recovers the same way`() {
        assertTrue(dlt.handler.handleOne(listenerFailed(InvalidEventException(RejectionReason.INVALID_VALUE)), dlt.record, dlt.consumer, dlt.container))

        assertEquals("invalid_value", dlt.singleDltRecord().headerText("x-rejection-reason"))
        assertEquals(listOf("rejected(invalid_value)"), dlt.metrics.outcomes)
    }
}
