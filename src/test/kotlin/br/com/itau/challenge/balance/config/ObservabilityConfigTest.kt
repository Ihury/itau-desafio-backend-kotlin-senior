package br.com.itau.challenge.balance.config

import br.com.itau.challenge.balance.testing.ManagedApplicationTest
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doReturn
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/** Metricas e Actuator na porta de gerenciamento; nada disso na porta da API. */
class ObservabilityConfigTest : ManagedApplicationTest() {
    @Autowired
    private lateinit var listeners: KafkaListenerEndpointRegistry

    private val account = "5b19c8b6-0cc4-4c72-a989-0c2ee15fa975"

    private fun scrapeAfterApiRequest(): String {
        doReturn(GetItemResponse.builder().build()).`when`(readClient).getItem(any(GetItemRequest::class.java))
        api("/balances/$account")
        var body = ""
        // o Boot registra o timer HTTP depois de a resposta ser enviada: espera a serie aparecer
        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            val response = management("/actuator/prometheus")
            assertEquals(200, response.statusCode())
            body = response.body()
            assertTrue(body.contains("http_server_requests_seconds_count"), "serie http_server_requests_seconds ainda ausente")
        }
        return body
    }

    private fun bucketBoundaries(
        prometheusText: String,
        metric: String,
    ): Set<Double> =
        // o valor da tag `uri` pode conter chaves (`/balances/{accountId}`): por isso `.*?` e nao `[^}]*`
        Regex("""^${Regex.escape(metric)}_bucket\{.*?le="([^"]+)"}""", RegexOption.MULTILINE)
            .findAll(prometheusText)
            .map { it.groupValues[1] }
            .filter { it != "+Inf" }
            .map { it.toDouble() }
            .toSet()

    @Test
    fun `the management port is different from the api port`() {
        assertNotEquals(apiPort, managementPort)
        assertTrue(managementPort > 0)
    }

    @Test
    fun `prometheus on the management port exposes the outcome counters`() {
        val body = scrapeAfterApiRequest()

        assertTrue(body.contains("balance_events_total{"), "balance_events_total ausente")
        assertTrue(body.contains("""outcome="processed""""))
        assertTrue(body.contains("""outcome="rejected""""))
    }

    @Test
    fun `http server requests carries the histogram with the 50 ms, 100 ms, 300 ms, 1 s and 2 s objectives`() {
        val boundaries = bucketBoundaries(scrapeAfterApiRequest(), "http_server_requests_seconds")

        listOf(0.05, 0.1, 0.3, 1.0, 2.0).forEach { assertTrue(it in boundaries, "bucket $it ausente em $boundaries") }
    }

    @Test
    fun `the business timers publish histogram buckets`() {
        val body = scrapeAfterApiRequest()

        listOf("balance_ingest_duration_seconds", "balance_store_write_duration_seconds", "balance_store_read_duration_seconds").forEach { metric ->
            val boundaries = bucketBoundaries(body, metric)
            assertTrue(0.005 in boundaries, "$metric sem bucket de 5 ms: $boundaries")
        }
        assertTrue(2.5 in bucketBoundaries(body, "balance_ingest_duration_seconds"))
    }

    @Test
    fun `the circuit breaker metrics are exported`() {
        assertTrue(scrapeAfterApiRequest().contains("resilience4j_circuitbreaker_state"))
    }

    @Test
    fun `actuator does not answer on the api port`() {
        listOf("/actuator", "/actuator/prometheus", "/actuator/health", "/actuator/health/liveness", "/actuator/health/readiness", "/actuator/info").forEach { path ->
            assertEquals(404, api(path).statusCode(), "$path nao pode responder na porta da API")
        }
    }

    @Test
    fun `only health, info and prometheus are exposed on the management port`() {
        assertEquals(200, management("/actuator/info").statusCode())
        listOf("/actuator/env", "/actuator/beans", "/actuator/metrics", "/actuator/loggers", "/actuator/heapdump").forEach { path ->
            assertEquals(404, management(path).statusCode(), "$path nao deve ser exposto")
        }
    }

    @Test
    fun `the kafka listener publishes observations`() {
        val container = assertNotNull(listeners.getListenerContainer("transaction-event-listener"))

        assertTrue(container.containerProperties.isObservationEnabled)
    }
}
