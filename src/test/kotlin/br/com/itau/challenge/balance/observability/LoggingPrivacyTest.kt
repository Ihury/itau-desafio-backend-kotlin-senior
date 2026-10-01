package br.com.itau.challenge.balance.observability

import br.com.itau.challenge.balance.adapter.input.kafka.BackOffProperties
import br.com.itau.challenge.balance.adapter.input.kafka.TransactionEventListener
import br.com.itau.challenge.balance.adapter.input.web.CorrelationIdFilter
import br.com.itau.challenge.balance.adapter.output.dynamodb.BalanceItemMapper
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.testing.DeadLetterHarness
import br.com.itau.challenge.balance.testing.ManagedApplicationTest
import br.com.itau.challenge.balance.testing.TRANSACTIONS_TOPIC
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_ACCOUNT_ID
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_TRANSACTION_ID
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.transactionEvent
import br.com.itau.challenge.balance.testing.TransactionPayloads
import br.com.itau.challenge.balance.testing.TransactionPayloads.quoted
import br.com.itau.challenge.balance.testing.aRecord
import br.com.itau.challenge.balance.testing.stringAttr
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.doThrow
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.io.PrintWriter
import java.io.StringWriter
import java.time.Clock
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@ExtendWith(OutputCaptureExtension::class)
class LoggingPrivacyTest : ManagedApplicationTest() {
    @Autowired
    private lateinit var listener: TransactionEventListener

    private val json = JsonMapper.builder().build()

    private val balanceSentinel = "98765.43"
    private val ownerSentinel = "0b5e1c2d-aaaa-4bbb-8ccc-1234567890ab"
    private val textSentinel = "SEGREDO-XYZ-123"
    private val malformedPayloadWithSentinel = """{"transaction": $textSentinel"""
    private val corruptedAccount = "aaaaaaaa-1111-4222-8333-bbbbbbbbbbbb"

    private fun payload(
        balance: String = balanceSentinel,
        owner: String = ownerSentinel,
        currency: String = "BRL",
    ) = TransactionPayloads.json(
        mapOf("account.balance.amount" to balance, "account.owner" to quoted(owner), "transaction.currency" to quoted(currency)),
    )

    private fun record(
        value: String,
        offset: Long,
    ) = ConsumerRecord<ByteArray?, ByteArray?>(TRANSACTIONS_TOPIC, 7, offset, null, value.toByteArray())

    private fun stackTraceOf(failure: Throwable): String = StringWriter().also { failure.printStackTrace(PrintWriter(it)) }.toString()

    private fun linesEmittedSince(
        output: CapturedOutput,
        testStart: Int,
    ): List<String> = output.all.substring(testStart).lines().filter { it.isNotBlank() }

    private fun parsed(line: String): JsonNode = json.readTree(line)

    private fun JsonNode.text(field: String): String? = this[field]?.asString()

    private fun assertNoSensitiveData(
        text: String,
        where: String,
    ) {
        listOf(balanceSentinel, "98765", ownerSentinel, textSentinel, "97.07", "1751641364589998").forEach { sentinel ->
            assertTrue(sentinel !in text, "'$sentinel' vazou para $where")
        }
    }

    private fun assertEveryLineIsJson(lines: List<String>): List<JsonNode> {
        assertTrue(lines.isNotEmpty(), "nenhum log capturado")
        lines.forEach { assertTrue(it.startsWith("{") && it.endsWith("}"), "linha nao e um objeto JSON: $it") }
        return lines.map { line -> runCatching { parsed(line) }.getOrElse { throw AssertionError("linha que nao e JSON: $line") } }
    }

    private fun ingestAppliedEvent() {
        listener.onMessage(record(payload(), offset = 10))
    }

    private fun ingestConflictingDuplicate() {
        val divergent =
            BalanceItemMapper.toItem(BalanceSnapshot.from(transactionEvent(transactionId = DEFAULT_TRANSACTION_ID, timestampMicros = 1751641364589998L, balanceAmount = "1.00")))
        doThrow(ConditionalCheckFailedException.builder().message("The conditional request failed").item(divergent).build())
            .`when`(writeClient)
            .updateItem(any(UpdateItemRequest::class.java))
        listener.onMessage(record(payload(), offset = 11))
    }

    private fun ingestRejectedEvents(): List<InvalidEventException> =
        listOf(
            malformedPayloadWithSentinel to 12L,
            payload(currency = textSentinel) to 13L,
            payload(owner = textSentinel) to 14L,
        ).map { (value, offset) -> assertFailsWith<InvalidEventException> { listener.onMessage(record(value, offset)) } }

    private fun requestBalanceWhileTheStoreIsDown() {
        doThrow(SdkClientException.builder().message("connect to dynamodb:8000 failed").build()).`when`(readClient).getItem(any(GetItemRequest::class.java))
        assertEquals(503, api("/balances/$DEFAULT_ACCOUNT_ID", CorrelationIdFilter.HEADER, "teste-123").statusCode())
    }

    private fun requestBalanceOfACorruptedItem() {
        val corrupted = BalanceItemMapper.toItem(BalanceSnapshot.from(transactionEvent())).toMutableMap()
        corrupted["accountStatus"] = stringAttr("SUSPENDED")
        doReturn(GetItemResponse.builder().item(corrupted).build()).`when`(readClient).getItem(any(GetItemRequest::class.java))
        assertEquals(500, api("/balances/$corruptedAccount", CorrelationIdFilter.HEADER, "teste-456").statusCode())
    }

    @Test
    fun `ingestion log lines are json objects carrying the correlation, account and transaction and nothing sensitive`(output: CapturedOutput) {
        val testStart = output.all.length

        ingestAppliedEvent()
        ingestConflictingDuplicate()
        ingestRejectedEvents()

        val emittedLines = linesEmittedSince(output, testStart)
        val records = assertEveryLineIsJson(emittedLines)
        assertNoSensitiveData(emittedLines.joinToString("\n"), "a saida de log")
        val applied = records.first { it.text("message")?.startsWith("event applied") == true }
        assertEquals("$TRANSACTIONS_TOPIC-7@10", applied.text("correlationId"))
        assertEquals(DEFAULT_ACCOUNT_ID, applied.text("accountId"))
        assertEquals(DEFAULT_TRANSACTION_ID, applied.text("transactionId"))
        val conflicting = records.first { it.text("message")?.startsWith("conflicting duplicate event") == true }
        assertEquals("$TRANSACTIONS_TOPIC-7@11", conflicting.text("correlationId"))
        assertEquals(DEFAULT_ACCOUNT_ID, conflicting.text("accountId"))
        assertEquals(DEFAULT_TRANSACTION_ID, conflicting.text("transactionId"))
    }

    @Test
    fun `rejected events carry no sensitive data in their stack traces`() {
        ingestRejectedEvents().forEach { assertNoSensitiveData(stackTraceOf(it), "a excecao do parser") }
    }

    @Test
    fun `api log lines are json objects carrying the client correlation and account and nothing sensitive`(output: CapturedOutput) {
        val testStart = output.all.length

        requestBalanceWhileTheStoreIsDown()
        requestBalanceOfACorruptedItem()

        val emittedLines = linesEmittedSince(output, testStart)
        val records = assertEveryLineIsJson(emittedLines)
        assertNoSensitiveData(emittedLines.joinToString("\n"), "a saida de log")
        val unavailable = records.first { it.text("message")?.startsWith("balance store unavailable") == true }
        assertEquals("teste-123", unavailable.text("correlationId"))
        assertEquals(DEFAULT_ACCOUNT_ID, unavailable.text("accountId"))
        val cannotMap = records.first { it.text("message")?.startsWith("balance item cannot be mapped") == true }
        assertEquals("teste-456", cannotMap.text("correlationId"))
        assertEquals(corruptedAccount, cannotMap.text("accountId"))
        val internal = records.first { it.text("message")?.startsWith("unexpected error while handling request") == true }
        assertEquals("teste-456", internal.text("correlationId"))
        assertNotNull(internal.text("accountId"))
    }

    @Test
    fun `the consumer error handler and spring kafka never log payload values, whatever the failure class`(output: CapturedOutput) {
        val testStart = output.all.length
        val dlt = DeadLetterHarness(backOff = BackOffProperties(initialMs = 10, maxMs = 20, jitterMs = 0), clock = Clock.systemUTC(), waitForSendResultTimeout = Duration.ofMillis(300))

        fun deliver(
            value: String,
            offset: Long,
            cause: Exception,
            times: Int,
        ) {
            val record = aRecord(value.toByteArray(), partition = 7, offset = offset)
            repeat(times) { dlt.deliver(record, cause) }
        }

        val invalid = assertFailsWith<InvalidEventException> { listener.onMessage(record(malformedPayloadWithSentinel, 20)) }
        deliver(malformedPayloadWithSentinel, 20, invalid, times = 1)
        deliver(payload(), 21, IllegalStateException("current balance item contradicts the failed condition"), times = 3)
        deliver(payload(), 22, BalanceStoreUnavailableException(StoreFailureCause.TIMEOUT, SdkClientException.builder().message("connect to dynamodb:8000 failed").build()), times = 2)

        val emitted = linesEmittedSince(output, testStart)
        assertTrue(emitted.isNotEmpty(), "o handler deveria ter registrado algo")
        emitted.forEach { line -> runCatching { parsed(line) }.getOrElse { throw AssertionError("linha que nao e JSON: $line") } }
        assertNoSensitiveData(emitted.joinToString("\n"), "o log do error handler e do Spring Kafka")
    }
}
