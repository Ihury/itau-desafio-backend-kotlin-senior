package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.support.DefectiveWriter
import br.com.itau.challenge.balance.support.DefectiveWriterConfig
import br.com.itau.challenge.balance.support.EventPayloads
import br.com.itau.challenge.balance.support.KafkaITBase
import br.com.itau.challenge.balance.support.PrometheusSample
import br.com.itau.challenge.balance.support.TopicSet
import br.com.itau.challenge.balance.support.TopicSet.Companion.SAME_PARTITION_KEY
import br.com.itau.challenge.balance.support.histogramQuantile
import br.com.itau.challenge.balance.support.singleValue
import br.com.itau.challenge.balance.support.sumOfSamples
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.context.annotation.Import
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import tools.jackson.databind.JsonNode
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@ExtendWith(OutputCaptureExtension::class)
@Import(DefectiveWriterConfig::class)
class ObservabilityIT : KafkaITBase() {
    override val topics: TopicSet
        get() = topicSet

    @Autowired
    private lateinit var writer: DefectiveWriter

    private fun eventsTotal(
        samples: List<PrometheusSample>,
        vararg labels: Pair<String, String>,
    ) = samples.sumOfSamples("balance_events_total", *labels)

    private fun delta(
        before: List<PrometheusSample>,
        after: List<PrometheusSample>,
        vararg labels: Pair<String, String>,
    ) = eventsTotal(after, *labels) - eventsTotal(before, *labels)

    private fun JsonNode.stringOrNull(field: String): String? = this[field]?.asString()

    private fun publishProcessedObsoleteAndDuplicate() {
        val accountA = newAccount()
        val accountB = newAccount()
        val olderEvent =
            EventPayloads.transaction(accountA, timestampMicros = EventPayloads.BASE_TIMESTAMP_MICROS, transactionId = OLDER_TRANSACTION_ID, balanceAmount = "10.00")
        val newerEvent =
            EventPayloads.transaction(accountA, timestampMicros = EventPayloads.BASE_TIMESTAMP_MICROS + 1_000, transactionId = NEWER_TRANSACTION_ID, balanceAmount = "20.00")
        listOf(olderEvent, newerEvent, olderEvent, newerEvent).forEach { topics.publishInPartitionOf(SAME_PARTITION_KEY, it) }
        topics.publish(EventPayloads.transaction(accountB, balanceAmount = "30.00"))
    }

    private fun publishOneRejectionOfEachReason() {
        val poisonedAccount = newAccount()
        writer.poisoned += poisonedAccount
        val malformedPayload = """{"account":{"owner":"$OWNER_SENTINEL","balance":{"amount":$BALANCE_SENTINEL,"note":"$TEXT_SENTINEL""""
        val missingFieldPayload =
            EventPayloads
                .transaction(newAccount(), balanceAmount = BALANCE_SENTINEL, ownerId = OWNER_SENTINEL)
                .replace(""""owner":"$OWNER_SENTINEL",""", "")
        val invalidIdentifierPayload = EventPayloads.transaction(newAccount(), transactionId = "1-1-1-1-1", balanceAmount = BALANCE_SENTINEL, ownerId = OWNER_SENTINEL)
        val invalidValuePayload = EventPayloads.transaction(newAccount(), balanceAmount = "\"$BALANCE_SENTINEL\"", ownerId = OWNER_SENTINEL)
        val invalidCurrencyPayload = EventPayloads.transaction(newAccount(), currency = "brl", balanceAmount = BALANCE_SENTINEL, ownerId = OWNER_SENTINEL)
        val invalidTimestampPayload = EventPayloads.transaction(newAccount(), timestampMicros = 1751749453433L, balanceAmount = BALANCE_SENTINEL, ownerId = OWNER_SENTINEL)
        val unknownDomainValuePayload = EventPayloads.transaction(newAccount(), transactionType = "TRANSFER", balanceAmount = BALANCE_SENTINEL, ownerId = OWNER_SENTINEL)
        val unprocessableEventPayload = EventPayloads.transaction(poisonedAccount, balanceAmount = BALANCE_SENTINEL, ownerId = OWNER_SENTINEL)
        listOf(
            malformedPayload,
            missingFieldPayload,
            invalidIdentifierPayload,
            invalidValuePayload,
            invalidCurrencyPayload,
            invalidTimestampPayload,
            unknownDomainValuePayload,
            unprocessableEventPayload,
        ).forEach { topics.publish(it) }
    }

    private fun publishBatch() {
        publishProcessedObsoleteAndDuplicate()
        publishOneRejectionOfEachReason()
    }

    private fun assertOutcomesReconcileWithTotalConsumed(
        before: List<PrometheusSample>,
        after: List<PrometheusSample>,
    ) {
        assertEquals(EXPECTED_PROCESSED.toDouble(), delta(before, after, "outcome" to "processed"), "processados")
        assertEquals(EXPECTED_OBSOLETE.toDouble(), delta(before, after, "outcome" to "obsolete"), "obsoletos")
        assertEquals(EXPECTED_DUPLICATE.toDouble(), delta(before, after, "outcome" to "duplicate"), "duplicados")
        assertEquals(EXPECTED_REJECTED.toDouble(), delta(before, after, "outcome" to "rejected"), "rejeitados")
        ALL_REJECTION_REASONS.forEach { assertEquals(1.0, delta(before, after, "outcome" to "rejected", "reason" to it), "rejeitado por $it") }
        assertEquals(PUBLISHED_MESSAGES.toDouble(), delta(before, after), "a soma dos desfechos e igual ao total consumido")
    }

    private fun assertRatiosMatchPromQlOfTheContract(
        before: List<PrometheusSample>,
        after: List<PrometheusSample>,
    ) {
        val rejectedRatio = delta(before, after, "outcome" to "rejected") / delta(before, after)
        val obsoleteRatio = delta(before, after, "outcome" to "obsolete") / delta(before, after)
        assertEquals(EXPECTED_REJECTED.toDouble() / PUBLISHED_MESSAGES, rejectedRatio, 1e-9)
        assertEquals(EXPECTED_OBSOLETE.toDouble() / PUBLISHED_MESSAGES, obsoleteRatio, 1e-9)
    }

    private fun assertIngestTimerCountsEveryDeliveryByOutcome(
        before: List<PrometheusSample>,
        after: List<PrometheusSample>,
    ) {
        fun ingested(outcome: String) =
            after.sumOfSamples("balance_ingest_duration_seconds_count", "outcome" to outcome) -
                before.sumOfSamples("balance_ingest_duration_seconds_count", "outcome" to outcome)
        assertEquals(
            mapOf("processed" to 3.0, "obsolete" to 1.0, "duplicate" to 1.0, "rejected" to 7.0),
            listOf("processed", "obsolete", "duplicate", "rejected").associateWith(::ingested),
        )
        assertTrue(
            ingested("error") >= 3.0,
            "o defeito interno tem no minimo 3 entregas (rebalance reinicia a contagem do DefaultErrorHandler e reentrega; ver UnclassifiedFailureIT), " +
                "mas rejected{unprocessable_event} segue exato: ${ingested("error")}",
        )
    }

    private fun assertLogsAreJsonAndLeakNothingFromThePayload(emittedLogs: String) {
        val emitted = emittedLogs.lines().filter { it.isNotBlank() }
        val records = emitted.map { line -> runCatching { json.readTree(line) }.getOrElse { throw AssertionError("linha que nao e JSON: $line") } }
        val text = emitted.joinToString("\n")
        listOf(BALANCE_SENTINEL, OWNER_SENTINEL, TEXT_SENTINEL, "97.07", "1751749453433").forEach { assertTrue(it !in text, "'$it' vazou para os logs") }
        assertTrue(records.any { it.stringOrNull("message") == "Record in retry and not yet recovered" }, "o retry da falha interna passa pelo Spring Kafka")
        records.filter { it.stringOrNull("logger_name") == "org.springframework.kafka.listener.KafkaMessageListenerContainer" }.forEach {
            assertTrue(it["stack_trace"] == null, "sem pilha (e sem a causa) nos logs do container: $it")
        }
        val isolated = records.filter { it.stringOrNull("message")?.startsWith("message isolated in the dlt") == true }
        assertEquals(EXPECTED_REJECTED, isolated.size)
        isolated.forEach { assertTrue(it.stringOrNull("correlationId")?.contains("@") == true, "correlacao topico-particao@offset: $it") }
        val applied = records.filter { it.stringOrNull("message")?.startsWith("event applied") == true }
        assertEquals(EXPECTED_PROCESSED, applied.size)
        applied.forEach {
            assertTrue(
                it.stringOrNull("accountId") != null && it.stringOrNull("transactionId") != null && it.stringOrNull("correlationId") != null,
                "contexto na ingestao: $it",
            )
        }
    }

    private fun awaitHttpTimerRegisteredAfterResponse() {
        await.atMost(HTTP_TIMER_REGISTRATION_TIMEOUT).untilAsserted {
            assertTrue(scrape().any { it.name == "http_server_requests_seconds_count" && it.labels["uri"] == "/balances/{accountId}" }, "serie http da consulta")
        }
    }

    @Test
    fun `every consumed message has exactly one counted outcome and the rejected and obsolete ratios are computable`(output: CapturedOutput) {
        val logOffset = output.all.length
        val before = scrape()

        publishBatch()

        topics.group.awaitLagZero(Duration.ofSeconds(60))
        val after = scrape()
        assertOutcomesReconcileWithTotalConsumed(before, after)
        assertRatiosMatchPromQlOfTheContract(before, after)
        assertIngestTimerCountsEveryDeliveryByOutcome(before, after)
        assertLogsAreJsonAndLeakNothingFromThePayload(output.all.substring(logOffset))
    }

    @Test
    fun `the contract metrics exist with their histograms and the circuit breaker state, and quantiles can be computed`() {
        val account = newAccount()
        publish(EventPayloads.transaction(account, balanceAmount = "44.00"))
        awaitBalance(account, "44.00")
        awaitHttpTimerRegisteredAfterResponse()
        val samples = scrape()

        fun bounds(
            metric: String,
            vararg labels: Pair<String, String>,
        ) = samples.filter { it.name == "${metric}_bucket" && labels.all { (key, value) -> it.labels[key] == value } }.map { it.labels.getValue("le") }.toSet()

        listOf(0.05, 0.1, 0.3, 1.0, 2.0).forEach { slo ->
            assertTrue(bounds("http_server_requests_seconds", "uri" to "/balances/{accountId}").filter { it != "+Inf" }.map { it.toDouble() }.contains(slo), "SLO $slo s da API")
        }
        listOf("balance_store_write_duration_seconds", "balance_store_read_duration_seconds", "balance_ingest_duration_seconds").forEach {
            assertTrue(bounds(it).size >= 9, "histograma de $it: ${bounds(it)}")
        }
        assertEquals(1.0, samples.singleValue("resilience4j_circuitbreaker_state", "name" to "dynamodb-read", "state" to "closed"), "circuit breaker fechado")
        assertTrue(samples.any { it.name == "resilience4j_circuitbreaker_calls_seconds_count" || it.name.startsWith("resilience4j_circuitbreaker_") }, "metricas do circuit breaker")
        assertTrue(samples.sumOfSamples("spring_kafka_listener_seconds_count") > 0, "observacao do listener Kafka (spring.kafka.listener.observation-enabled)")

        val writeP99 = samples.histogramQuantile(0.99, "balance_store_write_duration_seconds")
        val apiP99 = samples.histogramQuantile(0.99, "http_server_requests_seconds", "uri" to "/balances/{accountId}")
        assertTrue(writeP99.isFinite() && writeP99 > 0.0, "p99 da escrita: $writeP99")
        assertTrue(apiP99.isFinite() && apiP99 > 0.0, "p99 da API: $apiP99")
    }

    @Test
    fun `health is up on the management port, actuator is not served on the api port and the dependency gauge is 1`() {
        listOf("liveness", "readiness", "dependencies").forEach { group ->
            val response = management("/actuator/health/$group")
            assertEquals(200, response.statusCode(), group)
            assertEquals("""{"status":"UP"}""", response.body(), group)
        }
        assertEquals(1.0, scrape().singleValue("balance_dependency_up", "dependency" to "dynamodb"))
        listOf("/actuator/health", "/actuator/health/liveness", "/actuator/health/readiness", "/actuator/health/dependencies", "/actuator/prometheus", "/actuator/info").forEach {
            assertEquals(404, api(it).statusCode(), "$it nao pode responder na porta da API")
        }
    }

    companion object {
        private const val BALANCE_SENTINEL = "98765.43"
        private const val OWNER_SENTINEL = "0b5e1c2d-aaaa-4bbb-8ccc-1234567890ab"
        private const val TEXT_SENTINEL = "SEGREDO-OBS-999"
        private const val OLDER_TRANSACTION_ID = "11111111-1111-4111-8111-111111111111"
        private const val NEWER_TRANSACTION_ID = "22222222-2222-4222-8222-222222222222"
        private val ALL_REJECTION_REASONS =
            listOf("malformed_payload", "missing_field", "invalid_identifier", "invalid_value", "invalid_currency", "invalid_timestamp", "unknown_domain_value", "unprocessable_event")
        private const val EXPECTED_PROCESSED = 3
        private const val EXPECTED_OBSOLETE = 1
        private const val EXPECTED_DUPLICATE = 1
        private val EXPECTED_REJECTED = ALL_REJECTION_REASONS.size
        private val PUBLISHED_MESSAGES = EXPECTED_PROCESSED + EXPECTED_OBSOLETE + EXPECTED_DUPLICATE + EXPECTED_REJECTED
        private val HTTP_TIMER_REGISTRATION_TIMEOUT: Duration = Duration.ofSeconds(10)

        private val topicSet = TopicSet("it-obs")

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) = topicSet.registerProperties(registry)
    }
}
