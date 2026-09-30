package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import br.com.itau.challenge.balance.support.EventPayloads
import br.com.itau.challenge.balance.support.KafkaITBase
import br.com.itau.challenge.balance.support.PrometheusSample
import br.com.itau.challenge.balance.support.TopicSet
import br.com.itau.challenge.balance.support.histogramQuantile
import br.com.itau.challenge.balance.support.single
import br.com.itau.challenge.balance.support.sum
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import tools.jackson.databind.JsonNode
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Observabilidade ponta a ponta contra Redpanda e DynamoDB Local reais (US6, `quickstart.md` 2 e 9): um lote com TODOS os desfechos
 * (processado, obsoleto, duplicado e um rejeitado de cada motivo do catalogo) reconcilia com o total consumido (SC-010) lido em
 * `/actuator/prometheus` na porta de gerenciamento; as metricas do contrato (histogramas, circuit breaker) existem e as razoes de
 * rejeitados e obsoletos saem das consultas PromQL do contrato (SC-011); os logs do container real (inclusive o do Spring Kafka
 * para `RecordInRetryException`) sao JSON e nao carregam valores do payload; saude e Actuator ficam so na porta de gerenciamento.
 * Contexto e topicos proprios.
 */
@ExtendWith(OutputCaptureExtension::class)
class ObservabilityIT : KafkaITBase() {
    override val topics: TopicSet
        get() = topicSet

    /** Escritor que lanca um defeito interno para as contas marcadas (`unprocessable_event`); as demais vao ao DynamoDB real. */
    class DefectiveWriter(
        private val delegate: BalanceSnapshotWriter,
    ) : BalanceSnapshotWriter {
        val poisoned: MutableSet<String> = ConcurrentHashMap.newKeySet()

        override fun applyIfNewer(snapshot: BalanceSnapshot): ApplyResult {
            if (snapshot.accountId.value in poisoned) throw IllegalStateException("defeito simulado")
            return delegate.applyIfNewer(snapshot)
        }
    }

    @TestConfiguration
    class Config {
        @Bean
        @Primary
        fun defectiveWriter(
            @Qualifier("balanceSnapshotWriter") delegate: BalanceSnapshotWriter,
        ): DefectiveWriter = DefectiveWriter(delegate)
    }

    @Autowired
    private lateinit var writer: DefectiveWriter

    private val balanceSentinel = "98765.43"
    private val ownerSentinel = "0b5e1c2d-aaaa-4bbb-8ccc-1234567890ab"
    private val textSentinel = "SEGREDO-OBS-999"
    private val reasons =
        listOf("malformed_payload", "missing_field", "invalid_identifier", "invalid_value", "invalid_currency", "invalid_timestamp", "unknown_domain_value", "unprocessable_event")

    private fun events(
        samples: List<PrometheusSample>,
        vararg labels: Pair<String, String>,
    ) = samples.sum("balance_events_total", *labels)

    private fun JsonNode.text(field: String): String? = this[field]?.asString()

    /** Publica o lote: 3 processados (2 na conta A e 1 na B), 1 obsoleto, 1 duplicado e 8 rejeitados (um por motivo do catalogo). */
    private fun publishBatch(): Int {
        val accountA = newAccount()
        val accountB = newAccount()
        val txOld = "11111111-1111-4111-8111-111111111111"
        val txNew = "22222222-2222-4222-8222-222222222222"
        val old = EventPayloads.transaction(accountA, timestampMicros = EventPayloads.BASE_TIMESTAMP_MICROS, transactionId = txOld, balanceAmount = "10.00")
        val newer = EventPayloads.transaction(accountA, timestampMicros = EventPayloads.BASE_TIMESTAMP_MICROS + 1_000, transactionId = txNew, balanceAmount = "20.00")
        // mesma chave = mesma particao = ordem de publicacao: processado, processado, obsoleto (o vigente e mais novo) e duplicado
        listOf(old, newer, old, newer).forEach { topics.publishKeyed("obs-a", it) }
        topics.publish(EventPayloads.transaction(accountB, balanceAmount = "30.00"))

        val poisoned = newAccount()
        writer.poisoned += poisoned
        val hazardous = { balance: String -> EventPayloads.transaction(newAccount(), balanceAmount = balance, ownerId = ownerSentinel) }
        listOf(
            """{"account":{"owner":"$ownerSentinel","balance":{"amount":$balanceSentinel,"note":"$textSentinel"""", // malformado: JSON truncado
            hazardous(balanceSentinel).replace(""""owner":"$ownerSentinel",""", ""), // missing_field
            EventPayloads.transaction(newAccount(), transactionId = "1-1-1-1-1", balanceAmount = balanceSentinel, ownerId = ownerSentinel), // invalid_identifier
            EventPayloads.transaction(newAccount(), balanceAmount = "\"$balanceSentinel\"", ownerId = ownerSentinel), // invalid_value (texto)
            EventPayloads.transaction(newAccount(), balanceCurrency = "brl", balanceAmount = balanceSentinel, ownerId = ownerSentinel), // invalid_currency
            EventPayloads.transaction(newAccount(), timestampMicros = 1751749453433L, balanceAmount = balanceSentinel, ownerId = ownerSentinel), // invalid_timestamp
            EventPayloads.transaction(newAccount(), transactionType = "TRANSFER", balanceAmount = balanceSentinel, ownerId = ownerSentinel), // unknown_domain_value
            EventPayloads.transaction(poisoned, balanceAmount = balanceSentinel, ownerId = ownerSentinel), // unprocessable_event (defeito interno)
        ).forEach { topics.publish(it) }
        return 4 + 1 + 8
    }

    @Test
    fun `every consumed message has exactly one counted outcome and the rejected and obsolete ratios are computable (SC-010, SC-011)`(output: CapturedOutput) {
        val from = output.all.length
        val before = scrape()

        val published = publishBatch()

        topics.awaitLagZero(Duration.ofSeconds(60))
        val after = scrape()
        fun delta(vararg labels: Pair<String, String>) = events(after, *labels) - events(before, *labels)
        assertEquals(3.0, delta("outcome" to "processed"), "processados")
        assertEquals(1.0, delta("outcome" to "obsolete"), "obsoletos")
        assertEquals(1.0, delta("outcome" to "duplicate"), "duplicados")
        assertEquals(8.0, delta("outcome" to "rejected"), "rejeitados")
        reasons.forEach { assertEquals(1.0, delta("outcome" to "rejected", "reason" to it), "rejeitado por $it") }
        assertEquals(published.toDouble(), delta(), "a soma dos desfechos e igual ao total consumido (SC-010)")

        // SC-011: as consultas do contrato, sobre os deltas do periodo
        val rejectedRatio = delta("outcome" to "rejected") / delta()
        val obsoleteRatio = delta("outcome" to "obsolete") / delta()
        assertEquals(8.0 / published, rejectedRatio, 1e-9)
        assertEquals(1.0 / published, obsoleteRatio, 1e-9)

        // o timer de ingestao registra cada ENTREGA por desfecho: o defeito interno (nao classificado) tem 3 entregas, todas `error`
        fun ingested(outcome: String) = after.sum("balance_ingest_duration_seconds_count", "outcome" to outcome) - before.sum("balance_ingest_duration_seconds_count", "outcome" to outcome)
        assertEquals(mapOf("processed" to 3.0, "obsolete" to 1.0, "duplicate" to 1.0, "rejected" to 7.0, "error" to 3.0), listOf("processed", "obsolete", "duplicate", "rejected", "error").associateWith(::ingested))

        // privacidade dos logs do container real: JSON, sem valores do payload, com a mensagem do Spring Kafka para o retry
        val emitted = output.all.substring(from).lines().filter { it.isNotBlank() }
        val records = emitted.map { line -> runCatching { json.readTree(line) }.getOrElse { throw AssertionError("linha que nao e JSON: $line") } }
        val text = emitted.joinToString("\n")
        listOf(balanceSentinel, ownerSentinel, textSentinel, "97.07", "1751749453433").forEach { assertTrue(it !in text, "'$it' vazou para os logs") }
        assertTrue(records.any { it.text("message") == "Record in retry and not yet recovered" }, "o retry da falha interna passa pelo Spring Kafka")
        records.filter { it.text("logger_name") == "org.springframework.kafka.listener.KafkaMessageListenerContainer" }.forEach {
            assertTrue(it["stack_trace"] == null, "sem pilha (e sem a causa) nos logs do container: $it")
        }
        val isolated = records.filter { it.text("message")?.startsWith("message isolated in the dlt") == true }
        assertEquals(8, isolated.size)
        isolated.forEach { assertTrue(it.text("correlationId")?.contains("@") == true, "correlacao topico-particao@offset: $it") }
        val applied = records.filter { it.text("message")?.startsWith("event applied") == true }
        assertEquals(3, applied.size)
        applied.forEach { assertTrue(it.text("accountId") != null && it.text("transactionId") != null && it.text("correlationId") != null, "contexto na ingestao: $it") }
    }

    @Test
    fun `the contract metrics exist with their histograms and the circuit breaker state, and quantiles can be computed`() {
        val account = newAccount()
        publish(EventPayloads.transaction(account, balanceAmount = "44.00"))
        awaitBalance(account, "44.00")
        // o timer HTTP e registrado depois da resposta: espera a serie aparecer
        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            assertTrue(scrape().any { it.name == "http_server_requests_seconds_count" && it.labels["uri"] == "/balances/{accountId}" }, "serie http da consulta")
        }
        val samples = scrape()

        fun bounds(metric: String, vararg labels: Pair<String, String>) =
            samples.filter { it.name == "${metric}_bucket" && labels.all { (key, value) -> it.labels[key] == value } }.map { it.labels.getValue("le") }.toSet()

        listOf(0.05, 0.1, 0.3, 1.0, 2.0).forEach { slo ->
            assertTrue(bounds("http_server_requests_seconds", "uri" to "/balances/{accountId}").filter { it != "+Inf" }.map { it.toDouble() }.contains(slo), "SLO $slo s da API")
        }
        listOf("balance_store_write_duration_seconds", "balance_store_read_duration_seconds", "balance_ingest_duration_seconds").forEach {
            assertTrue(bounds(it).size >= 9, "histograma de $it: ${bounds(it)}")
        }
        assertEquals(1.0, samples.single("resilience4j_circuitbreaker_state", "name" to "dynamodb-read", "state" to "closed"), "circuit breaker fechado")
        assertTrue(samples.any { it.name == "resilience4j_circuitbreaker_calls_seconds_count" || it.name.startsWith("resilience4j_circuitbreaker_") }, "metricas do circuit breaker")
        assertTrue(samples.sum("spring_kafka_listener_seconds_count") > 0, "observacao do listener Kafka (spring.kafka.listener.observation-enabled)")

        // histogram_quantile do contrato (p99 da escrita e da API)
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
        assertEquals(1.0, scrape().single("balance_dependency_up", "dependency" to "dynamodb"))
        listOf("/actuator/health", "/actuator/health/liveness", "/actuator/health/readiness", "/actuator/health/dependencies", "/actuator/prometheus", "/actuator/info").forEach {
            assertEquals(404, api(it).statusCode(), "$it nao pode responder na porta da API")
        }
    }

    companion object {
        private val topicSet = TopicSet("it-obs")

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) = topicSet.registerProperties(registry)
    }
}
