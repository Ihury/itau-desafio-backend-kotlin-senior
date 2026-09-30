package br.com.itau.challenge.balance.adapter.input.web

import br.com.itau.challenge.balance.adapter.input.web.dto.BalanceResponse
import br.com.itau.challenge.balance.domain.exception.AccountDisabledException
import br.com.itau.challenge.balance.domain.exception.AccountNotFoundException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.domain.model.TransactionEventFixtures.DEFAULT_ACCOUNT_ID
import br.com.itau.challenge.balance.domain.model.TransactionEventFixtures.transactionEvent
import br.com.itau.challenge.balance.port.input.GetBalanceUseCase
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.doThrow
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.MvcResult
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.yaml.snakeyaml.Yaml
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.math.BigDecimal
import java.nio.file.Files
import java.nio.file.Path
import java.time.OffsetDateTime
import java.time.ZoneId
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Anti-drift do contrato: o documento servido (`static/openapi.yaml`, lido SOMENTE do classpath, porque o estagio `test`
 * do Dockerfile nao copia `specs/`) e verificado contra as respostas reais do controller.
 */
@WebMvcTest(BalanceController::class)
@Import(ProblemDetailsAdvice::class, CorrelationIdFilter::class, WebTestBeans::class)
class OpenApiContractTest {
    @Autowired
    private lateinit var mockMvc: MockMvc

    @MockitoBean
    private lateinit var getBalance: GetBalanceUseCase

    private val json = JsonMapper.builder().build()
    private val accountId = AccountId.parse(DEFAULT_ACCOUNT_ID)
    private val snapshot = BalanceSnapshot.from(transactionEvent())

    private val document: Map<String, Any?> by lazy {
        val stream = assertNotNull(javaClass.getResourceAsStream("/static/openapi.yaml"), "static/openapi.yaml ausente do classpath")
        @Suppress("UNCHECKED_CAST")
        stream.use { Yaml().load<Any>(it) as Map<String, Any?> }
    }

    @Suppress("UNCHECKED_CAST")
    private fun Any?.asMap(): Map<String, Any?> = this as Map<String, Any?>

    @Suppress("UNCHECKED_CAST")
    private fun Any?.asList(): List<Any?> = this as List<Any?>

    /** Segue `$ref: '#/components/...'` ate o objeto. */
    private fun resolve(node: Any?): Map<String, Any?> {
        var current = node.asMap()
        while (current.containsKey("\$ref")) {
            var target: Any? = document
            (current["\$ref"] as String).removePrefix("#/").split('/').forEach { target = target.asMap()[it] }
            current = target.asMap()
        }
        return current
    }

    private val operation: Map<String, Any?> get() = document["paths"].asMap()["/balances/{accountId}"].asMap()["get"].asMap()

    private val responses: Map<String, Any?> get() = operation["responses"].asMap()

    private fun performRequestProducing(status: String): MvcResult {
        when (status) {
            "200" -> doReturn(snapshot).`when`(getBalance).getBalance(accountId)
            "404" -> doThrow(AccountNotFoundException(accountId)).`when`(getBalance).getBalance(accountId)
            "409" -> doThrow(AccountDisabledException(accountId)).`when`(getBalance).getBalance(accountId)
            "500" -> doThrow(RuntimeException("falha")).`when`(getBalance).getBalance(accountId)
            "503" -> doThrow(BalanceStoreUnavailableException(StoreFailureCause.UNAVAILABLE)).`when`(getBalance).getBalance(accountId)
        }
        val path = if (status == "400") "/balances/abc" else "/balances/$DEFAULT_ACCOUNT_ID"
        return mockMvc.perform(get(path)).andReturn()
    }

    private fun bodyOf(result: MvcResult): JsonNode = json.readTree(result.response.contentAsString)

    private fun mediaTypeOf(status: String): String = resolve(responses[status]).let { it["content"].asMap().keys.single() }

    @Test
    fun `the operation documents exactly the statuses 200, 400, 404, 409, 500 and 503`() {
        assertEquals(setOf("200", "400", "404", "409", "500", "503"), responses.keys)
    }

    @Test
    fun `each documented status matches the real response in status and media type`() {
        responses.keys.forEach { status ->
            val result = performRequestProducing(status)

            assertEquals(status.toInt(), result.response.status, "status $status")
            val expected = MediaType.parseMediaType(mediaTypeOf(status))
            val actual = MediaType.parseMediaType(result.response.contentType!!)
            assertTrue(expected.isCompatibleWith(actual) && actual.subtype == expected.subtype, "media type de $status: $actual x $expected")
        }
    }

    @Test
    fun `problem responses match type, status, required properties and declare no extra properties`() {
        val problem = resolve(mapOf("\$ref" to "#/components/schemas/Problem"))
        val required = problem["required"].asList().map { it as String }
        val allowed = problem["properties"].asMap().keys

        responses.keys.filter { it != "200" }.forEach { status ->
            val schema = resolve(resolve(responses[status])["content"].asMap()["application/problem+json"].asMap()["schema"])
            val constants = schema["allOf"].asList()[1].asMap()["properties"].asMap()
            val body = bodyOf(performRequestProducing(status))

            assertEquals(constants["type"].asMap()["const"], body["type"].asString(), "type de $status")
            assertEquals(constants["status"].asMap()["const"], body["status"].asInt(), "status de $status")
            required.forEach { assertTrue(body.has(it), "propriedade obrigatoria $it ausente em $status") }
            val extra = body.propertyNames().toSet() - allowed
            assertTrue(extra.isEmpty(), "propriedades fora do schema em $status: $extra")
        }
    }

    @Test
    fun `the 200 body matches BalanceResponse with additionalProperties false`() {
        val schema = resolve(mapOf("\$ref" to "#/components/schemas/BalanceResponse"))
        val money = resolve(mapOf("\$ref" to "#/components/schemas/Money"))
        assertEquals(false, schema["additionalProperties"])
        assertEquals(false, money["additionalProperties"])

        val body = bodyOf(performRequestProducing("200"))

        assertEquals(schema["required"].asList().toSet(), body.propertyNames().toSet())
        assertEquals(schema["properties"].asMap().keys, body.propertyNames().toSet())
        assertEquals(money["required"].asList().toSet(), body["balance"].propertyNames().toSet())
        assertEquals(money["properties"].asMap().keys, body["balance"].propertyNames().toSet())
        assertTrue(body["balance"]["amount"].isNumber)
    }

    @Test
    fun `the 409 response carries no balance, owner or update time`() {
        val body = bodyOf(performRequestProducing("409"))

        listOf("balance", "owner", "updated_at").forEach { assertFalse(body.has(it), "$it nao pode aparecer no 409") }
    }

    @Test
    fun `declared response headers are present with the documented values`() {
        responses.keys.forEach { status ->
            val result = performRequestProducing(status)
            val headers = resolve(responses[status])["headers"]?.asMap().orEmpty()

            assertTrue(headers.isNotEmpty(), "todo status declara ao menos X-Correlation-Id: $status")
            headers.forEach { (name, definition) ->
                val value = result.response.getHeader(name)
                assertNotNull(value, "header $name declarado e ausente em $status")
                val schema = resolve(definition)["schema"].asMap()
                schema["const"]?.let { assertEquals(it, value, "valor do header $name em $status") }
                if (schema["type"] == "integer") {
                    assertTrue(value.toLong() >= (schema["minimum"] as Int).toLong(), "$name abaixo do minimo")
                    assertEquals("10", value, "Retry-After = espera do circuit breaker")
                }
            }
        }
        assertTrue("Cache-Control" in resolve(responses["200"])["headers"].asMap())
        assertTrue("Retry-After" in resolve(responses["503"])["headers"].asMap())
    }

    @Test
    fun `X-Correlation-Id is declared in every response and its pattern is the filter pattern`() {
        responses.keys.forEach { assertTrue("X-Correlation-Id" in resolve(responses[it])["headers"].asMap(), "X-Correlation-Id em $it") }
        val parameter = operation["parameters"].asList().map { it.asMap() }.single { it["name"] == "X-Correlation-Id" }
        val schema = parameter["schema"].asMap()

        assertEquals(CorrelationIdFilter.PATTERN, schema["pattern"])
        assertEquals(64, schema["maxLength"])
        assertEquals(false, parameter["required"])
    }

    @Test
    fun `the accountId pattern accepts and rejects exactly what the controller accepts and rejects`() {
        val parameter = operation["parameters"].asList().map { it.asMap() }.single { it["name"] == "accountId" }
        val pattern = Regex(parameter["schema"].asMap()["pattern"] as String)
        val samples =
            listOf(
                DEFAULT_ACCOUNT_ID, DEFAULT_ACCOUNT_ID.uppercase(), "abc", "1-1-1-1-1", "", " $DEFAULT_ACCOUNT_ID", "$DEFAULT_ACCOUNT_ID ",
                DEFAULT_ACCOUNT_ID.replace("-", ""), "{$DEFAULT_ACCOUNT_ID}", "urn:uuid:$DEFAULT_ACCOUNT_ID", DEFAULT_ACCOUNT_ID.dropLast(1),
                DEFAULT_ACCOUNT_ID.dropLast(1) + "g", "00000000-0000-0000-0000-000000000000", "ffffffff-FFFF-ffff-FFFF-ffffffffffff",
            )

        samples.forEach { sample ->
            val controllerAccepts = runCatching { AccountId.parse(sample) }.isSuccess
            assertEquals(controllerAccepts, pattern.matches(sample), "divergencia para '$sample'")
        }
    }

    @Test
    fun `the 200 examples of the document match the real format`() {
        val examples = resolve(responses["200"])["content"].asMap()["application/json"].asMap()["examples"].asMap()
        assertTrue(examples.isNotEmpty())

        examples.values.forEach { example ->
            val value = example.asMap()["value"].asMap()
            val balance = value["balance"].asMap()
            val instant = OffsetDateTime.parse(value["updated_at"] as String).toInstant()
            val micros = instant.epochSecond * MICROS_PER_SECOND + instant.nano / NANOS_PER_MICRO
            val amount = BigDecimal(balance["amount"].toString())
            val real =
                BalanceResponse.from(
                    BalanceSnapshot.from(
                        transactionEvent(
                            accountId = value["id"] as String,
                            ownerId = value["owner"] as String,
                            balanceAmount = amount.toPlainString(),
                            balanceCurrency = balance["currency"] as String,
                            timestampMicros = micros,
                        ),
                    ),
                    ZoneId.of("America/Sao_Paulo"),
                )

            assertEquals(value["id"], real.id)
            assertEquals(value["owner"], real.owner)
            assertEquals(balance["currency"], real.balance.currency)
            assertEquals(0, amount.compareTo(real.balance.amount), "o exemplo ${balance["amount"]} difere do valor real ${real.balance.amount}")
            assertEquals(value["updated_at"], real.updatedAt)
        }
    }

    @Test
    fun `the problem examples of the document match title, detail and instance of the real responses`() {
        responses.keys.filter { it != "200" }.forEach { status ->
            val example = resolve(responses[status])["content"].asMap()["application/problem+json"].asMap()["example"].asMap()
            val body = bodyOf(performRequestProducing(status))

            assertEquals(example["type"], body["type"].asString(), "type do exemplo de $status")
            assertEquals(example["title"], body["title"].asString(), "title do exemplo de $status")
            assertEquals(example["status"], body["status"].asInt(), "status do exemplo de $status")
            assertEquals(example["detail"], body["detail"].asString(), "detail do exemplo de $status")
            assertEquals(example["instance"], body["instance"].asString(), "instance do exemplo de $status")
        }
    }

    @Test
    fun `an InvalidEventException escaping the use case is documented as the generic 500`() {
        doThrow(InvalidEventException(RejectionReason.INVALID_VALUE)).`when`(getBalance).getBalance(accountId)

        val body = bodyOf(mockMvc.perform(get("/balances/$DEFAULT_ACCOUNT_ID")).andReturn())

        assertEquals(500, body["status"].asInt())
    }

    @Test
    fun `the copy served from the classpath is identical to the contract in specs when specs exists`() {
        val contract = Path.of("specs/001-consulta-saldo/contracts/openapi.yaml")
        assumeTrue(Files.exists(contract), "specs/ nao esta disponivel neste ambiente (estagio de teste do Docker)")

        val served = assertNotNull(javaClass.getResourceAsStream("/static/openapi.yaml")).use { it.readAllBytes() }

        assertTrue(Files.readAllBytes(contract).contentEquals(served), "static/openapi.yaml difere do contrato em specs/")
    }

    private companion object {
        const val MICROS_PER_SECOND = 1_000_000L
        const val NANOS_PER_MICRO = 1_000L
    }
}
