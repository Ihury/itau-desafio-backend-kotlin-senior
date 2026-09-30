package br.com.itau.challenge.balance.support

import io.micrometer.core.instrument.MeterRegistry
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalManagementPort
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import tools.jackson.databind.DeserializationFeature
import tools.jackson.databind.json.JsonMapper
import java.math.BigDecimal
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import kotlin.test.assertEquals

/**
 * Comportamento comum dos ITs de ingestao ponta a ponta (Redpanda e DynamoDB Local reais): consulta HTTP, contas de teste e
 * espera de saldo. O conjunto de topicos e grupo ([topics]) e definido pela subclasse.
 *
 * Um IT com contexto Spring PROPRIO (outras propriedades ou `@TestConfiguration`) estende esta classe com um [TopicSet] proprio e
 * a sua `@DynamicPropertySource`; os ITs sem contexto proprio estendem [KafkaIngestionITBase], que compartilha um unico contexto
 * (dois contextos no mesmo grupo dividiriam as particoes e um deles processaria as mensagens do outro).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
abstract class KafkaITBase {
    companion object {
        /** SLO de consulta apos a publicacao (SC-002). */
        val SLO: Duration = Duration.ofSeconds(5)
    }

    /** Topicos e grupo do contexto deste IT. */
    protected abstract val topics: TopicSet

    @LocalServerPort
    protected var port: Int = 0

    /** Porta do Actuator (health e prometheus), separada da API. */
    @LocalManagementPort
    protected var managementPort: Int = 0

    @Autowired
    protected lateinit var registry: KafkaListenerEndpointRegistry

    @Autowired
    protected lateinit var meterRegistry: MeterRegistry

    private val http = HttpClient.newHttpClient()
    protected val json: JsonMapper = JsonMapper.builder().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).build()
    protected lateinit var raw: DynamoDbClient
    private val created = mutableListOf<String>()

    @BeforeEach
    fun setUp() {
        raw = DynamoDbTestSupport.rawClient()
        topics.awaitAssignment(registry)
    }

    @AfterEach
    fun tearDown() {
        created.forEach { raw.deleteItem(DeleteItemRequest.builder().tableName(DynamoDbTestSupport.tableName).key(DynamoDbTestSupport.key(it)).build()) }
        created.clear()
        raw.close()
    }

    protected fun newAccount(): String = DynamoDbTestSupport.randomAccountId().also { created += it }

    protected fun get(accountId: String): HttpResponse<String> =
        http.send(HttpRequest.newBuilder(URI.create("http://localhost:$port/balances/$accountId")).GET().build(), HttpResponse.BodyHandlers.ofString())

    /** GET na porta da API, em um caminho qualquer. */
    protected fun api(path: String): HttpResponse<String> =
        http.send(HttpRequest.newBuilder(URI.create("http://localhost:$port$path")).GET().build(), HttpResponse.BodyHandlers.ofString())

    /** GET na porta de gerenciamento (Actuator). */
    protected fun management(path: String): HttpResponse<String> =
        http.send(HttpRequest.newBuilder(URI.create("http://localhost:$managementPort$path")).GET().build(), HttpResponse.BodyHandlers.ofString())

    /** Amostras de `/actuator/prometheus` na porta de gerenciamento. */
    protected fun scrape(): List<PrometheusSample> {
        val response = management("/actuator/prometheus")
        assertEquals(200, response.statusCode(), "GET /actuator/prometheus")
        return PrometheusSample.parse(response.body())
    }

    protected fun publish(payload: String) = topics.publish(payload)

    /** Espera, em ate [SLO] apos a publicacao, a consulta responder 200 com [amount] e [owner]. */
    protected fun awaitBalance(
        accountId: String,
        amount: String,
        owner: String = EventPayloads.DEFAULT_OWNER,
        updatedAt: String? = null,
    ) {
        await.atMost(SLO).untilAsserted {
            val response = get(accountId)
            assertEquals(200, response.statusCode(), "status da consulta de $accountId")
            val body = json.readTree(response.body())
            assertEquals(0, BigDecimal(amount).compareTo(body["balance"]["amount"].decimalValue()), "saldo esperado $amount, veio ${body["balance"]["amount"]}")
            assertEquals(owner, body["owner"].asString())
            updatedAt?.let { assertEquals(it, body["updated_at"].asString()) }
        }
    }
}

/**
 * Base dos ITs de ingestao que compartilham UM contexto Spring em cache: a `@DynamicPropertySource` vive AQUI, de modo que todas
 * as subclasses usam o mesmo conjunto [IntegrationInfra.shared] e um unico listener no grupo de consumo exclusivo da execucao.
 */
abstract class KafkaIngestionITBase : KafkaITBase() {
    override val topics: TopicSet
        get() = IntegrationInfra.shared

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) = IntegrationInfra.registerProperties(registry)
    }
}
