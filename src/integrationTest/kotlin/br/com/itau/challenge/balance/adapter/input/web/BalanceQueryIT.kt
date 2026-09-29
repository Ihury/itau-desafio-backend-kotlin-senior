package br.com.itau.challenge.balance.adapter.input.web

import br.com.itau.challenge.balance.support.DynamoDbTestSupport
import br.com.itau.challenge.balance.support.DynamoDbTestSupport.item
import br.com.itau.challenge.balance.support.DynamoDbTestSupport.randomAccountId
import io.micrometer.core.instrument.MeterRegistry
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import tools.jackson.databind.json.JsonMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Consulta ponta a ponta contra o DynamoDB Local real (`make integration-test`): HTTP real (porta aleatoria), controller,
 * caso de uso, circuit breaker, cliente de leitura e banco. Cada teste usa uma conta aleatoria (o item do seed e so lido).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class BalanceQueryIT {
    @LocalServerPort
    private var port: Int = 0

    @MockitoSpyBean(name = "dynamoDbReadClient")
    private lateinit var readClient: DynamoDbClient

    @Autowired
    private lateinit var meterRegistry: MeterRegistry

    private val http = HttpClient.newHttpClient()
    private val json = JsonMapper.builder().build()
    private lateinit var raw: DynamoDbClient
    private val created = mutableListOf<String>()

    @BeforeEach
    fun setUp() {
        raw = DynamoDbTestSupport.rawClient()
    }

    @AfterEach
    fun tearDown() {
        created.forEach { raw.deleteItem(DeleteItemRequest.builder().tableName(DynamoDbTestSupport.tableName).key(DynamoDbTestSupport.key(it)).build()) }
        raw.close()
    }

    private fun newAccount(vararg overrides: Pair<String, AttributeValue>): String {
        val id = randomAccountId()
        store(id, *overrides)
        created += id
        return id
    }

    private fun store(
        accountId: String,
        vararg overrides: Pair<String, AttributeValue>,
    ) {
        val base = item(accountId)
        raw.putItem(PutItemRequest.builder().tableName(DynamoDbTestSupport.tableName).item(base + overrides).build())
    }

    private fun s(value: String): AttributeValue = AttributeValue.builder().s(value).build()

    private fun n(value: String): AttributeValue = AttributeValue.builder().n(value).build()

    private fun get(
        path: String,
        vararg headers: String,
    ): HttpResponse<String> {
        val request = HttpRequest.newBuilder(URI.create("http://localhost:$port$path")).GET().apply { if (headers.isNotEmpty()) headers(*headers) }.build()
        return http.send(request, HttpResponse.BodyHandlers.ofString())
    }

    private fun HttpResponse<String>.header(name: String): String? = headers().firstValue(name).orElse(null)

    private fun assertNoDatabaseRead() = verify(readClient, never()).getItem(any(GetItemRequest::class.java))

    @Test
    fun `the seed item answers exactly the example of the client`() {
        val response = get("/balances/5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")

        assertEquals(200, response.statusCode())
        assertEquals(
            """{"id":"5b19c8b6-0cc4-4c72-a989-0c2ee15fa975","owner":"315e3cfe-f4af-4cd2-b298-a449e614349a",""" +
                """"balance":{"amount":183.12,"currency":"BRL"},"updated_at":"2025-07-05T18:04:13.433-03:00"}""",
            response.body(),
        )
        assertEquals("no-store", response.header("Cache-Control"))
        assertTrue(response.header("Content-Type")!!.startsWith("application/json"))
        assertNotNull(response.header("X-Correlation-Id"))
    }

    @Test
    fun `a balance stored as 183 point 1 is answered with the currency scale completed`() {
        val id = newAccount("balanceAmount" to n("183.10"))

        val body = get("/balances/$id").body()

        assertTrue(""""balance":{"amount":183.10,"currency":"BRL"}""" in body, body)
    }

    @Test
    fun `a disabled account is a 409 with no balance fields`() {
        val id = newAccount("accountStatus" to s("DISABLED"), "balanceAmount" to n("999.99"))

        val response = get("/balances/$id")

        assertEquals(409, response.statusCode())
        assertTrue(response.header("Content-Type")!!.startsWith("application/problem+json"))
        val body = json.readTree(response.body())
        assertEquals("urn:problem-type:consulta-saldo:conta-desabilitada", body["type"].asString())
        listOf("balance", "owner", "updated_at").forEach { assertFalse(body.has(it), "$it no 409") }
        assertFalse("999.99" in response.body())
    }

    @Test
    fun `an account without a snapshot is a 404 and never a zero balance`() {
        val response = get("/balances/${randomAccountId()}")

        assertEquals(404, response.statusCode())
        assertEquals("urn:problem-type:consulta-saldo:conta-nao-encontrada", json.readTree(response.body())["type"].asString())
        assertFalse("amount" in response.body())
    }

    @Test
    fun `malformed ids are a 400 and the database is never called`() {
        listOf("abc", "1-1-1-1-1").forEach { bad ->
            val response = get("/balances/$bad")

            assertEquals(400, response.statusCode(), bad)
            assertEquals("urn:problem-type:consulta-saldo:requisicao-invalida", json.readTree(response.body())["type"].asString())
        }
        assertNoDatabaseRead()
    }

    @Test
    fun `a disabled snapshot replaced by a newer enabled one makes the query succeed`() {
        val id = newAccount("accountStatus" to s("DISABLED"))
        assertEquals(409, get("/balances/$id").statusCode())

        store(
            id,
            "accountStatus" to s("ENABLED"),
            "balanceAmount" to n("70"),
            "lastTxTsMicros" to n("1751749454433000"),
            "lastTxId" to s("00000000-0000-4000-8000-000000000032"),
        )
        val response = get("/balances/$id")

        assertEquals(200, response.statusCode())
        assertTrue(""""amount":70.00""" in response.body(), response.body())
    }

    @Test
    fun `microseconds of the event are preserved in updated_at`() {
        val id = newAccount("lastTxTsMicros" to n("1751749453433123"))

        assertTrue(""""updated_at":"2025-07-05T18:04:13.433123-03:00"""" in get("/balances/$id").body())
    }

    @Test
    fun `the balance currency is exposed as stored without conversion`() {
        val id = newAccount("balanceCurrency" to s("USD"), "balanceAmount" to n("25.00"))

        assertTrue(""""balance":{"amount":25.00,"currency":"USD"}""" in get("/balances/$id").body())
    }

    @Test
    fun `the correlation id is echoed on success and on error`() {
        val id = newAccount()

        assertEquals("teste-123", get("/balances/$id", "X-Correlation-Id", "teste-123").header("X-Correlation-Id"))
        assertEquals("teste-456", get("/balances/${randomAccountId()}", "X-Correlation-Id", "teste-456").header("X-Correlation-Id"))
    }

    @Test
    fun `a corrupted item is an internal error with no data and is counted`() {
        val id = newAccount("accountStatus" to s("SUSPENDED"), "balanceAmount" to n("98765.43"))
        val before = meterRegistry.counter("balance.store.read.corrupted").count()

        val response = get("/balances/$id")

        assertEquals(500, response.statusCode())
        assertEquals("urn:problem-type:consulta-saldo:erro-interno", json.readTree(response.body())["type"].asString())
        assertFalse("98765.43" in response.body() || "SUSPENDED" in response.body())
        assertEquals(before + 1, meterRegistry.counter("balance.store.read.corrupted").count())
    }

    @Test
    fun `a database failure is a 503 with retry after and never a 404`() {
        val id = newAccount()
        doThrow(SdkClientException.builder().message("Unable to execute HTTP request").build()).`when`(readClient).getItem(any(GetItemRequest::class.java))

        val response = get("/balances/$id")

        assertEquals(503, response.statusCode())
        assertEquals("10", response.header("Retry-After"))
        assertEquals("urn:problem-type:consulta-saldo:servico-indisponivel", json.readTree(response.body())["type"].asString())
    }
}
