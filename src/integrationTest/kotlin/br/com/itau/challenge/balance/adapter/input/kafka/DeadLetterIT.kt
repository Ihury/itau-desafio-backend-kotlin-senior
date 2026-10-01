package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.support.Defect
import br.com.itau.challenge.balance.support.KafkaITBase
import br.com.itau.challenge.balance.support.TopicSet
import br.com.itau.challenge.balance.support.oneDefectOfEachKind
import br.com.itau.challenge.balance.support.processedCount
import br.com.itau.challenge.balance.support.publishValidBatch
import br.com.itau.challenge.balance.support.rejectedCount
import br.com.itau.challenge.balance.support.validPayload
import br.com.itau.challenge.balance.testing.toEpochMicros
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.time.Duration
import java.time.Instant
import java.util.Base64
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DeadLetterIT : KafkaITBase() {
    override val topics: TopicSet
        get() = topicSet

    private fun ConsumerRecord<ByteArray, ByteArray>.header(name: String): String? = headers().lastHeader(name)?.value()?.toString(Charsets.UTF_8)

    private fun base64(bytes: ByteArray): String = Base64.getEncoder().encodeToString(bytes)

    private fun beyondFutureTolerance(): Long = Instant.now().plus(BEYOND_FUTURE_TOLERANCE).toEpochMicros()

    private fun assertDltHeaders(record: ConsumerRecord<ByteArray, ByteArray>) {
        assertNotNull(record.header("x-rejection-reason"))
        Instant.parse(assertNotNull(record.header("x-rejected-at")))
        assertEquals(topics.topic, record.header("kafka_dlt-original-topic"))
        assertNotNull(record.header("kafka_dlt-original-partition"))
        assertNotNull(record.header("kafka_dlt-original-offset"))
        assertNotNull(record.header("kafka_dlt-original-timestamp"))
        val names = record.headers().map { it.key() }
        assertTrue(names.none { it.startsWith("kafka_dlt-exception") }, "header de excecao vazou: $names")
    }

    private fun assertRecordIsolatedAs(
        record: ConsumerRecord<ByteArray, ByteArray>,
        reason: String,
        detailPath: String,
    ) {
        assertEquals(reason, record.header("x-rejection-reason"))
        assertEquals(detailPath, record.header("x-rejection-detail"))
    }

    @Test
    fun `one defective message of each kind interleaved with valid ones is isolated by reason with the original bytes while the valid ones are processed`() {
        val dltOffsetsBefore = topics.dlt.endOffsets()
        val expectedRejectionsByReason = mapOf("malformed_payload" to 1, "missing_field" to 1, "invalid_identifier" to 1, "invalid_currency" to 1, "invalid_value" to 1, "invalid_timestamp" to 2, "unknown_domain_value" to 2)
        val rejectedBefore = expectedRejectionsByReason.keys.associateWith { meterRegistry.rejectedCount(it) }
        val processedBefore = meterRegistry.processedCount()
        val defects = oneDefectOfEachKind(::newAccount)
        val validAccount = newAccount()
        val futureWithinTolerance = newAccount()
        val withinFutureTolerance = Instant.now().plus(WITHIN_FUTURE_TOLERANCE).toEpochMicros()

        defects.take(4).forEach { topics.publish(it.payload) }
        topics.publish(validPayload(validAccount))
        defects.drop(4).forEach { topics.publish(it.payload) }
        topics.publish(validPayload(futureWithinTolerance, timestampMicros = withinFutureTolerance, amount = "9.00"))

        awaitBalance(validAccount, "5.00")
        awaitBalance(futureWithinTolerance, "9.00")
        topics.group.awaitLagZero()

        assertEquals(defects.size, topics.dlt.countSince(dltOffsetsBefore), "o DLT recebe exatamente as ${defects.size} defeituosas")
        val records = topics.dlt.recordsSince(dltOffsetsBefore)
        assertEquals(defects.size, records.size)
        records.forEach(::assertDltHeaders)
        assertEquals(expectedRejectionsByReason, records.mapNotNull { it.header("x-rejection-reason") }.groupingBy { it }.eachCount())
        assertEquals(defects.map { base64(it.payload) }.sorted(), records.map { base64(it.value()) }.sorted(), "valor do DLT == bytes originais; o detalhe e so o caminho do campo")
        defects.forEach { defect ->
            val record = assertNotNull(records.singleOrNull { it.value().contentEquals(defect.payload) }, defect.description)
            assertEquals(defect.expectedReason, record.header("x-rejection-reason"), defect.description)
            assertEquals(defect.expectedDetailPath, record.header("x-rejection-detail"), defect.description)
        }
        defects.mapNotNull { it.account }.forEach { assertEquals(404, get(it).statusCode(), "conta $it deveria seguir inexistente") }
        assertEquals(processedBefore + 2, meterRegistry.processedCount())
        expectedRejectionsByReason.forEach { (reason, delta) -> assertEquals(rejectedBefore.getValue(reason) + delta, meterRegistry.rejectedCount(reason), reason) }
    }

    @Test
    fun `an account created in 1998 is valid and one created in 1850 is isolated as invalid timestamp`() {
        val dltOffsetsBefore = topics.dlt.endOffsets()
        val created1998Account = newAccount()
        val tooOld = newAccount()
        val oldPayload = validPayload(created1998Account, amount = "15.00", createdAtMicros = CREATED_IN_1998_MICROS)
        val tooOldPayload = validPayload(tooOld, createdAtMicros = ONE_MICRO_BEFORE_1900_MICROS)

        topics.publish(oldPayload)
        topics.publish(tooOldPayload)

        awaitBalance(created1998Account, "15.00")
        topics.group.awaitLagZero()
        val records = topics.dlt.recordsSince(dltOffsetsBefore)
        assertEquals(1, records.size, "so a de 1850 vai ao DLT")
        val record = records.single()
        assertRecordIsolatedAs(record, "invalid_timestamp", "account.created_at")
        assertEquals(tooOldPayload, record.value().toString(Charsets.UTF_8))
        assertEquals(404, get(tooOld).statusCode())
    }

    @Test
    fun `a transaction timestamp beyond the future tolerance is isolated without touching the balance`() {
        val dltOffsetsBefore = topics.dlt.endOffsets()
        val account = newAccount()
        val payload = validPayload(account, timestampMicros = beyondFutureTolerance())

        topics.publish(payload)

        topics.group.awaitLagZero()
        val record = topics.dlt.recordsSince(dltOffsetsBefore).single()
        assertRecordIsolatedAs(record, "invalid_timestamp", "transaction.timestamp")
        assertEquals(payload, record.value().toString(Charsets.UTF_8))
        assertEquals(404, get(account).statusCode())
    }

    @Test
    fun `an account creation beyond the future tolerance is isolated with the account created at path`() {
        val dltOffsetsBefore = topics.dlt.endOffsets()
        val account = newAccount()
        val payload = validPayload(account, createdAtMicros = beyondFutureTolerance())

        topics.publish(payload)

        topics.group.awaitLagZero()
        val record = topics.dlt.recordsSince(dltOffsetsBefore).single()
        assertRecordIsolatedAs(record, "invalid_timestamp", "account.created_at")
        assertEquals(404, get(account).statusCode())
    }

    @Test
    fun `binary bytes and a message over 64 KiB reach the dlt exactly as published`() {
        val dltOffsetsBefore = topics.dlt.endOffsets()
        val binary = byteArrayOf('{'.code.toByte(), 0xC3.toByte(), 0x28, 0x00, 0xFF.toByte(), 0x7F)
        val huge = (validPayload(newAccount()) + " ".repeat(OVERSIZED_PADDING_BYTES)).toByteArray(Charsets.UTF_8)

        topics.publish(binary)
        topics.publish(huge)

        topics.group.awaitLagZero()
        val records = topics.dlt.recordsSince(dltOffsetsBefore)
        assertEquals(2, records.size)
        assertEquals(listOf(base64(binary), base64(huge)).sorted(), records.map { base64(it.value()) }.sorted())
        records.forEach {
            assertEquals("malformed_payload", it.header("x-rejection-reason"))
            assertNull(it.headers().lastHeader("x-rejection-detail"), "sem caminho de campo para payload malformado")
            assertDltHeaders(it)
        }
    }

    @Test
    fun `defective messages interleaved with many valid ones never block them, all valid are processed and only the defective reach the dlt`() {
        val dltOffsetsBefore = topics.dlt.endOffsets()
        val processedBefore = meterRegistry.processedCount()
        val defects: List<Defect> = oneDefectOfEachKind(::newAccount)

        publishValidBatch(topics, ::newAccount, FUNCTIONAL_VALID_MESSAGES, defects)

        await.atMost(Duration.ofSeconds(60)).untilAsserted { assertEquals(processedBefore + FUNCTIONAL_VALID_MESSAGES, meterRegistry.processedCount(), "validas processadas") }
        topics.group.awaitLagZero(Duration.ofSeconds(60))
        assertEquals(defects.size, topics.dlt.countSince(dltOffsetsBefore), "o DLT recebe exatamente as ${defects.size} defeituosas e nenhuma valida")
        assertEquals(defects.map { base64(it.payload) }.sorted(), topics.dlt.recordsSince(dltOffsetsBefore).map { base64(it.value()) }.sorted())
    }

    companion object {
        private const val FUNCTIONAL_VALID_MESSAGES = 300
        private const val OVERSIZED_PADDING_BYTES = 70 * 1024
        private const val CREATED_IN_1998_MICROS = 899_251_200_000_000L
        private const val ONE_MICRO_BEFORE_1900_MICROS = -2_208_988_800_000_001L
        private val BEYOND_FUTURE_TOLERANCE: Duration = Duration.ofMinutes(6)
        private val WITHIN_FUTURE_TOLERANCE: Duration = Duration.ofSeconds(60)

        private val topicSet = TopicSet("it-dlt")

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) = topicSet.registerProperties(registry)
    }
}
