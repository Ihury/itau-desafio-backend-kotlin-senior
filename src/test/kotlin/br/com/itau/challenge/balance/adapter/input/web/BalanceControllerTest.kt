package br.com.itau.challenge.balance.adapter.input.web

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
import org.junit.jupiter.api.Test
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.verifyNoInteractions
import org.slf4j.MDC
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.context.annotation.Import
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.MvcResult
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.header
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

@WebMvcTest(BalanceController::class)
@Import(ProblemDetailsAdvice::class, CorrelationIdFilter::class, WebTestBeans::class)
class BalanceControllerTest {
    @Autowired
    private lateinit var mockMvc: MockMvc

    @MockitoBean
    private lateinit var getBalance: GetBalanceUseCase

    private val accountId = AccountId.parse(DEFAULT_ACCOUNT_ID)
    private val snapshot = BalanceSnapshot.from(transactionEvent())

    private fun typeOf(slug: String) = "urn:problem-type:consulta-saldo:$slug"

    private fun MvcResult.header(name: String): String? = response.getHeader(name)

    @Test
    fun `200 returns the balance with json, no-store and a correlation id`() {
        doReturn(snapshot).`when`(getBalance).getBalance(accountId)

        val result =
            mockMvc
                .perform(get("/balances/{id}", DEFAULT_ACCOUNT_ID))
                .andExpect(status().isOk)
                .andExpect(header().string(HttpHeaders.CACHE_CONTROL, "no-store"))
                .andExpect(header().exists(CorrelationIdFilter.HEADER))
                .andReturn()

        assertTrue(MediaType.APPLICATION_JSON.isCompatibleWith(MediaType.parseMediaType(result.response.contentType!!)))
        assertEquals(
            """{"id":"$DEFAULT_ACCOUNT_ID","owner":"315e3cfe-f4af-4cd2-b298-a449e614349a",""" +
                """"balance":{"amount":183.12,"currency":"BRL"},"updated_at":"2025-07-05T18:04:13.433-03:00"}""",
            result.response.contentAsString,
        )
    }

    @Test
    fun `amount is written as plain decimal completed to the currency digits (spring jackson configuration)`() {
        val sci = BalanceSnapshot.from(transactionEvent(balanceAmount = "1E+3"))
        doReturn(sci).`when`(getBalance).getBalance(accountId)

        mockMvc
            .perform(get("/balances/{id}", DEFAULT_ACCOUNT_ID))
            .andExpect(status().isOk)
            .andExpect(content().string(org.hamcrest.Matchers.containsString("\"amount\":1000.00,")))
    }

    @Test
    fun `400 for malformed ids and the use case is never invoked`() {
        listOf("abc", "1-1-1-1-1", "5b19c8b6-0cc4-4c72-a989-0c2ee15fa97", "5b19c8b6-0cc4-4c72-a989-0c2ee15fa97g").forEach { bad ->
            mockMvc
                .perform(get("/balances/{id}", bad))
                .andExpect(status().isBadRequest)
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.type").value(typeOf("requisicao-invalida")))
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(header().exists(CorrelationIdFilter.HEADER))
        }
        assertEquals(35, "5b19c8b6-0cc4-4c72-a989-0c2ee15fa97".length)
        verifyNoInteractions(getBalance)
    }

    @Test
    fun `an uppercase account id is accepted and forwarded in lowercase`() {
        doReturn(snapshot).`when`(getBalance).getBalance(accountId)

        mockMvc.perform(get("/balances/{id}", DEFAULT_ACCOUNT_ID.uppercase())).andExpect(status().isOk)
    }

    @Test
    fun `404 when the account has no snapshot`() {
        doThrow(AccountNotFoundException(accountId)).`when`(getBalance).getBalance(accountId)

        mockMvc
            .perform(get("/balances/{id}", DEFAULT_ACCOUNT_ID))
            .andExpect(status().isNotFound)
            .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
            .andExpect(jsonPath("$.type").value(typeOf("conta-nao-encontrada")))
            .andExpect(jsonPath("$.title").value("Conta não encontrada"))
            .andExpect(jsonPath("$.status").value(404))
            .andExpect(jsonPath("$.detail").exists())
            .andExpect(jsonPath("$.instance").value("/balances/$DEFAULT_ACCOUNT_ID"))
            .andExpect(header().exists(CorrelationIdFilter.HEADER))
    }

    @Test
    fun `409 for a disabled account with a body that carries no balance data`() {
        doThrow(AccountDisabledException(accountId)).`when`(getBalance).getBalance(accountId)

        val body =
            mockMvc
                .perform(get("/balances/{id}", DEFAULT_ACCOUNT_ID))
                .andExpect(status().isConflict)
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.type").value(typeOf("conta-desabilitada")))
                .andExpect(jsonPath("$.status").value(409))
                .andExpect(jsonPath("$.balance").doesNotExist())
                .andExpect(jsonPath("$.owner").doesNotExist())
                .andExpect(jsonPath("$.updated_at").doesNotExist())
                .andReturn()
                .response
                .contentAsString

        assertFalse("amount" in body || "owner" in body || "updated_at" in body)
    }

    @Test
    fun `503 with retry after when the store is unavailable`() {
        doThrow(BalanceStoreUnavailableException(StoreFailureCause.TIMEOUT)).`when`(getBalance).getBalance(accountId)

        mockMvc
            .perform(get("/balances/{id}", DEFAULT_ACCOUNT_ID))
            .andExpect(status().isServiceUnavailable)
            .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
            .andExpect(header().string(HttpHeaders.RETRY_AFTER, "10"))
            .andExpect(jsonPath("$.type").value(typeOf("servico-indisponivel")))
            .andExpect(jsonPath("$.status").value(503))
            .andExpect(header().exists(CorrelationIdFilter.HEADER))
    }

    @Test
    fun `500 for an unexpected failure with no stack trace and no internal message`() {
        doThrow(RuntimeException("tabela AccountBalances")).`when`(getBalance).getBalance(accountId)

        val body =
            mockMvc
                .perform(get("/balances/{id}", DEFAULT_ACCOUNT_ID))
                .andExpect(status().isInternalServerError)
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.type").value(typeOf("erro-interno")))
                .andExpect(jsonPath("$.status").value(500))
                .andExpect(header().exists(CorrelationIdFilter.HEADER))
                .andReturn()
                .response
                .contentAsString

        assertFalse("AccountBalances" in body)
        assertFalse("RuntimeException" in body || "at br.com" in body || "stackTrace" in body)
    }

    @Test
    fun `a corrupted item reported as IllegalStateException is a 500 and never a 400 or a 404`() {
        doThrow(IllegalStateException("corrupted balance item: invalid attribute 'ownerId'")).`when`(getBalance).getBalance(accountId)

        val body =
            mockMvc
                .perform(get("/balances/{id}", DEFAULT_ACCOUNT_ID))
                .andExpect(status().isInternalServerError)
                .andExpect(jsonPath("$.type").value(typeOf("erro-interno")))
                .andReturn()
                .response
                .contentAsString

        assertFalse("ownerId" in body && "corrupted" in body)
    }

    @Test
    fun `an InvalidEventException escaping the use case is an internal error and never a 400`() {
        doThrow(InvalidEventException(RejectionReason.INVALID_VALUE)).`when`(getBalance).getBalance(accountId)

        mockMvc
            .perform(get("/balances/{id}", DEFAULT_ACCOUNT_ID))
            .andExpect(status().isInternalServerError)
            .andExpect(jsonPath("$.type").value(typeOf("erro-interno")))
    }

    @Test
    fun `problem details carry status title detail and instance`() {
        doThrow(AccountNotFoundException(accountId)).`when`(getBalance).getBalance(accountId)

        mockMvc
            .perform(get("/balances/{id}", DEFAULT_ACCOUNT_ID))
            .andExpect(jsonPath("$.type").isString)
            .andExpect(jsonPath("$.title").isString)
            .andExpect(jsonPath("$.status").isNumber)
            .andExpect(jsonPath("$.detail").isString)
            .andExpect(jsonPath("$.instance").isString)
    }

    @Test
    fun `a valid correlation id is echoed`() {
        doReturn(snapshot).`when`(getBalance).getBalance(accountId)

        mockMvc
            .perform(get("/balances/{id}", DEFAULT_ACCOUNT_ID).header(CorrelationIdFilter.HEADER, "teste-123.abc_9"))
            .andExpect(header().string(CorrelationIdFilter.HEADER, "teste-123.abc_9"))
    }

    @Test
    fun `an invalid correlation id is replaced by a generated uuid`() {
        doReturn(snapshot).`when`(getBalance).getBalance(accountId)

        listOf("bad id!", "x".repeat(65), "").forEach { invalid ->
            val echoed =
                mockMvc
                    .perform(get("/balances/{id}", DEFAULT_ACCOUNT_ID).header(CorrelationIdFilter.HEADER, invalid))
                    .andReturn()
                    .header(CorrelationIdFilter.HEADER)
            assertNotEquals(invalid, echoed)
            assertTrue(Regex("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$").matches(echoed!!), "gerado: $echoed")
        }
    }

    @Test
    fun `a missing correlation id is generated and error responses carry it too`() {
        doThrow(AccountNotFoundException(accountId)).`when`(getBalance).getBalance(accountId)

        val notFound = mockMvc.perform(get("/balances/{id}", DEFAULT_ACCOUNT_ID)).andReturn().header(CorrelationIdFilter.HEADER)
        val badRequest = mockMvc.perform(get("/balances/abc")).andReturn().header(CorrelationIdFilter.HEADER)

        assertTrue(!notFound.isNullOrBlank() && !badRequest.isNullOrBlank())
        assertNotEquals(notFound, badRequest)
    }

    @Test
    fun `the logging context is always cleaned after the request`() {
        doReturn(snapshot).`when`(getBalance).getBalance(accountId)
        mockMvc.perform(get("/balances/{id}", DEFAULT_ACCOUNT_ID).header(CorrelationIdFilter.HEADER, "abc"))
        assertNull(MDC.get("correlationId"))
        assertNull(MDC.get("accountId"))

        doThrow(RuntimeException("x")).`when`(getBalance).getBalance(accountId)
        mockMvc.perform(get("/balances/{id}", DEFAULT_ACCOUNT_ID))
        assertNull(MDC.get("correlationId"))
        assertNull(MDC.get("accountId"))
    }

    @Test
    fun `unknown route and unsupported method are problem json`() {
        mockMvc
            .perform(get("/nao-existe"))
            .andExpect(status().isNotFound)
            .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
            .andExpect(header().exists(CorrelationIdFilter.HEADER))

        mockMvc
            .perform(post("/balances/{id}", DEFAULT_ACCOUNT_ID))
            .andExpect(status().isMethodNotAllowed)
            .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
    }
}
