package br.com.itau.challenge.balance.support

import io.micrometer.core.instrument.MeterRegistry
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalManagementPort
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import tools.jackson.databind.DeserializationFeature
import tools.jackson.databind.json.JsonMapper
import java.math.BigDecimal
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import kotlin.test.assertEquals

// Contexto Spring proprio exige TopicSet proprio: dois contextos no mesmo grupo dividem as particoes.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@ExtendWith(AwaitilityDefaults::class)
abstract class KafkaITBase {
    companion object {
        val PUBLISH_TO_QUERY_SLO: Duration = Duration.ofSeconds(5)
    }

    protected abstract val topics: TopicSet

    @LocalServerPort
    protected var port: Int = 0

    @LocalManagementPort
    protected var managementPort: Int = 0

    @Autowired
    protected lateinit var registry: KafkaListenerEndpointRegistry

    @Autowired
    protected lateinit var meterRegistry: MeterRegistry

    private val http = HttpClient.newHttpClient()
    protected val json: JsonMapper = JsonMapper.builder().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).build()
    protected lateinit var raw: DynamoDbClient
    private val createdAccountIds = mutableListOf<String>()

    @BeforeEach
    fun setUp() {
        raw = DynamoDbTestSupport.rawClient()
        topics.group.awaitStabilized(registry)
        awaitPreviousTestFullyCommitted()
    }

    @AfterEach
    fun tearDown() {
        createdAccountIds.forEach { DynamoDbTestSupport.deleteAccount(raw, it) }
        createdAccountIds.clear()
        raw.close()
    }

    private fun awaitPreviousTestFullyCommitted() = topics.group.awaitLagZero()

    protected fun newAccount(): String = DynamoDbTestSupport.randomAccountId().also { createdAccountIds += it }

    private fun send(
        port: Int,
        path: String,
    ): HttpResponse<String> = http.send(HttpRequest.newBuilder(URI.create("http://localhost:$port$path")).GET().build(), HttpResponse.BodyHandlers.ofString())

    protected fun get(accountId: String): HttpResponse<String> = send(port, "/balances/$accountId")

    protected fun api(path: String): HttpResponse<String> = send(port, path)

    protected fun management(path: String): HttpResponse<String> = send(managementPort, path)

    protected fun scrape(): List<PrometheusSample> {
        val response = management("/actuator/prometheus")
        assertEquals(200, response.statusCode(), "GET /actuator/prometheus")
        return PrometheusSample.parse(response.body())
    }

    protected fun publish(payload: String) = topics.publish(payload)

    protected fun awaitBalance(
        accountId: String,
        amount: String,
        owner: String = EventPayloads.DEFAULT_OWNER,
        updatedAt: String? = null,
    ) {
        await.atMost(PUBLISH_TO_QUERY_SLO).untilAsserted {
            val response = get(accountId)
            assertEquals(200, response.statusCode(), "status da consulta de $accountId")
            val body = json.readTree(response.body())
            assertEquals(0, BigDecimal(amount).compareTo(body["balance"]["amount"].decimalValue()), "saldo esperado $amount, veio ${body["balance"]["amount"]}")
            assertEquals(owner, body["owner"].asString())
            updatedAt?.let { assertEquals(it, body["updated_at"].asString()) }
        }
    }
}

abstract class SharedContextKafkaITBase : KafkaITBase() {
    override val topics: TopicSet
        get() = IntegrationInfra.sharedTopics

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) = IntegrationInfra.registerSharedProperties(registry)
    }
}
