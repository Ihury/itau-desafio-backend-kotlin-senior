package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.support.DynamoDbTestSupport
import br.com.itau.challenge.balance.support.EventPayloads
import br.com.itau.challenge.balance.support.IntegrationInfra
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
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
import java.util.UUID
import kotlin.test.assertEquals

/**
 * Ingestao ponta a ponta com Redpanda e DynamoDB Local reais (`make integration-test`): publica no topico exclusivo, o
 * listener real consome, o servico grava e a consulta HTTP real reflete o saldo. Cada teste usa contas aleatorias. O saldo
 * precisa estar consultavel em ate 5 s apos a publicacao em cada caso (SC-002).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class TransactionEventIngestionIT {
    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) = IntegrationInfra.registerProperties(registry)

        private val SLO = Duration.ofSeconds(5)
    }

    @LocalServerPort
    private var port: Int = 0

    @Autowired
    private lateinit var registry: KafkaListenerEndpointRegistry

    private val http = HttpClient.newHttpClient()
    private val json = JsonMapper.builder().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).build()
    private lateinit var raw: DynamoDbClient
    private val created = mutableListOf<String>()

    @BeforeEach
    fun setUp() {
        raw = DynamoDbTestSupport.rawClient()
        IntegrationInfra.awaitAssignment(registry)
    }

    @AfterEach
    fun tearDown() {
        created.forEach { raw.deleteItem(DeleteItemRequest.builder().tableName(DynamoDbTestSupport.tableName).key(DynamoDbTestSupport.key(it)).build()) }
        raw.close()
    }

    private fun newAccount(): String = DynamoDbTestSupport.randomAccountId().also { created += it }

    private fun get(accountId: String): HttpResponse<String> =
        http.send(HttpRequest.newBuilder(URI.create("http://localhost:$port/balances/$accountId")).GET().build(), HttpResponse.BodyHandlers.ofString())

    private fun publish(payload: String) = IntegrationInfra.publish(payload)

    /** Espera, em ate 5 s apos a publicacao, a consulta responder 200 com [amount] e [owner]. */
    private fun awaitBalance(
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

    @Test
    fun `a new account is created by the first event and reflected by the query`() {
        val account = newAccount()
        assertEquals(404, get(account).statusCode())

        publish(EventPayloads.transaction(account, balanceAmount = "183.12"))

        awaitBalance(account, "183.12", updatedAt = "2025-07-05T18:04:13.433-03:00")
    }

    @Test
    fun `a newer event replaces balance, owner and instant`() {
        val account = newAccount()
        publish(EventPayloads.transaction(account, timestampMicros = 1751749453433000L, balanceAmount = "100.00"))
        awaitBalance(account, "100.00")

        val newOwner = UUID.randomUUID().toString()
        publish(EventPayloads.transaction(account, timestampMicros = 1751749460000000L, balanceAmount = "250.50", ownerId = newOwner))

        awaitBalance(account, "250.50", owner = newOwner, updatedAt = "2025-07-05T18:04:20-03:00")
    }

    @Test
    fun `an older event does not replace the snapshot`() {
        val account = newAccount()
        publish(EventPayloads.transaction(account, timestampMicros = 1751749460000000L, balanceAmount = "250.50"))
        awaitBalance(account, "250.50")

        publish(EventPayloads.transaction(account, timestampMicros = 1751749453433000L, balanceAmount = "1.00"))
        val marker = newAccount()
        publish(EventPayloads.transaction(marker, balanceAmount = "5.00"))
        awaitBalance(marker, "5.00") // o marcador processado depois do evento antigo prova que ele ja foi consumido

        awaitBalance(account, "250.50")
    }

    @Test
    fun `events of interleaved accounts do not interfere with one another`() {
        val first = newAccount()
        val second = newAccount()

        publish(EventPayloads.transaction(first, balanceAmount = "10.00"))
        publish(EventPayloads.transaction(second, balanceAmount = "20.00"))
        publish(EventPayloads.transaction(first, timestampMicros = 1751749454000000L, balanceAmount = "11.00"))
        publish(EventPayloads.transaction(second, timestampMicros = 1751749455000000L, balanceAmount = "21.00"))

        awaitBalance(first, "11.00")
        awaitBalance(second, "21.00")
    }

    @Test
    fun `microseconds of the event are kept in updated_at`() {
        val account = newAccount()

        publish(EventPayloads.transaction(account, timestampMicros = 1751749453433123L))

        awaitBalance(account, "183.12", updatedAt = "2025-07-05T18:04:13.433123-03:00")
    }

    @Test
    fun `a newer declined event updates the snapshot like any other`() {
        val account = newAccount()
        publish(EventPayloads.transaction(account, timestampMicros = 1751749453433000L, balanceAmount = "100.00"))
        awaitBalance(account, "100.00")

        publish(EventPayloads.transaction(account, timestampMicros = 1751749453433001L, balanceAmount = "77.00", transactionStatus = "DECLINED"))

        awaitBalance(account, "77.00", updatedAt = "2025-07-05T18:04:13.433001-03:00")
    }

    @Test
    fun `an event that disables the account makes the query answer conflict`() {
        val account = newAccount()
        publish(EventPayloads.transaction(account, timestampMicros = 1751749453433000L, balanceAmount = "100.00"))
        awaitBalance(account, "100.00")

        publish(EventPayloads.transaction(account, timestampMicros = 1751749453433001L, accountStatus = "DISABLED"))

        await.atMost(SLO).untilAsserted { assertEquals(409, get(account).statusCode()) }
    }
}
