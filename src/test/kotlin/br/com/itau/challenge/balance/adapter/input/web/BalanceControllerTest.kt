package br.com.itau.challenge.balance.adapter.input.web

import br.com.itau.challenge.balance.domain.exception.AccountDisabledException
import br.com.itau.challenge.balance.domain.exception.AccountNotFoundException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.testing.LogCapture
import br.com.itau.challenge.balance.testing.RETRY_AFTER_SECONDS
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_ACCOUNT_ID
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_OWNER_ID
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.transactionEvent
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.verifyNoInteractions
import org.slf4j.MDC
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
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

class BalanceControllerTest : WebSliceTest() {
    @JvmField
    @RegisterExtension
    val advice = LogCapture(ProblemDetailsAdvice::class.java)

    @Test
    fun `an existing account is answered with the balance as json, no-store and a correlation id`() {
        doReturn(snapshot).`when`(getBalance).getBalance(accountId)

        val result =
            requestBalance()
                .andExpect(status().isOk)
                .andExpect(header().string(HttpHeaders.CACHE_CONTROL, "no-store"))
                .andExpect(header().exists(CorrelationIdFilter.HEADER))
                .andReturn()

        assertTrue(MediaType.APPLICATION_JSON.isCompatibleWith(MediaType.parseMediaType(result.response.contentType!!)))
        assertEquals(
            """{"id":"$DEFAULT_ACCOUNT_ID","owner":"$DEFAULT_OWNER_ID",""" +
                """"balance":{"amount":183.12,"currency":"BRL"},"updated_at":"2025-07-05T18:04:13.433-03:00"}""",
            result.response.contentAsString,
        )
    }

    @Test
    fun `amount is written as plain decimal completed to the currency digits`() {
        val scientificNotationSnapshot = BalanceSnapshot.from(transactionEvent(balanceAmount = "1E+3"))
        doReturn(scientificNotationSnapshot).`when`(getBalance).getBalance(accountId)

        requestBalance()
            .andExpect(status().isOk)
            .andExpect(content().string(containsString("\"amount\":1000.00,")))
    }

    @Test
    fun `malformed ids are answered as bad request and the use case is never invoked`() {
        val oneCharacterShort = DEFAULT_ACCOUNT_ID.dropLast(1)
        listOf("abc", "1-1-1-1-1", oneCharacterShort, oneCharacterShort + "g").forEach { bad ->
            requestBalance(bad).andExpectProblem(HttpStatus.BAD_REQUEST, "requisicao-invalida")
        }
        verifyNoInteractions(getBalance)
    }

    @Test
    fun `an uppercase account id is accepted and forwarded in lowercase`() {
        doReturn(snapshot).`when`(getBalance).getBalance(accountId)

        requestBalance(DEFAULT_ACCOUNT_ID.uppercase()).andExpect(status().isOk)
    }

    @Test
    fun `an account without snapshot is answered as not found`() {
        doThrow(AccountNotFoundException(accountId)).`when`(getBalance).getBalance(accountId)

        requestBalance()
            .andExpectProblem(HttpStatus.NOT_FOUND, "conta-nao-encontrada")
            .andExpect(jsonPath("$.title").value("Conta não encontrada"))
            .andExpect(jsonPath("$.detail").exists())
            .andExpect(jsonPath("$.instance").value("/balances/$DEFAULT_ACCOUNT_ID"))
    }

    @Test
    fun `a disabled account is answered as conflict with a body that carries no balance data`() {
        doThrow(AccountDisabledException(accountId)).`when`(getBalance).getBalance(accountId)

        val body =
            requestBalance()
                .andExpectProblem(HttpStatus.CONFLICT, "conta-desabilitada")
                .andExpect(jsonPath("$.balance").doesNotExist())
                .andExpect(jsonPath("$.owner").doesNotExist())
                .andExpect(jsonPath("$.updated_at").doesNotExist())
                .andReturn()
                .response
                .contentAsString

        assertFalse("amount" in body || "owner" in body || "updated_at" in body)
    }

    @Test
    fun `an unavailable store is answered as service unavailable with retry after`() {
        doThrow(BalanceStoreUnavailableException(StoreFailureCause.TIMEOUT)).`when`(getBalance).getBalance(accountId)

        requestBalance()
            .andExpectProblem(HttpStatus.SERVICE_UNAVAILABLE, "servico-indisponivel")
            .andExpect(header().string(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS))
    }

    @Test
    fun `an unexpected failure is answered as internal error with no stack trace and no internal message`() {
        doThrow(RuntimeException("tabela AccountBalances")).`when`(getBalance).getBalance(accountId)

        val body =
            requestBalance()
                .andExpectProblem(HttpStatus.INTERNAL_SERVER_ERROR, "erro-interno")
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
            requestBalance()
                .andExpectProblem(HttpStatus.INTERNAL_SERVER_ERROR, "erro-interno")
                .andReturn()
                .response
                .contentAsString

        assertFalse("ownerId" in body || "corrupted" in body)
    }

    @Test
    fun `an InvalidEventException escaping the use case is an internal error and never a 400`() {
        doThrow(InvalidEventException(RejectionReason.INVALID_VALUE)).`when`(getBalance).getBalance(accountId)

        requestBalance().andExpectProblem(HttpStatus.INTERNAL_SERVER_ERROR, "erro-interno")
    }

    @Test
    fun `problem details carry status title detail and instance`() {
        doThrow(AccountNotFoundException(accountId)).`when`(getBalance).getBalance(accountId)

        requestBalance()
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
            assertTrue(LOWERCASE_UUID.matches(echoed!!), "gerado: $echoed")
        }
    }

    @Test
    fun `a missing correlation id is generated and error responses carry it too`() {
        doThrow(AccountNotFoundException(accountId)).`when`(getBalance).getBalance(accountId)

        val notFound = requestBalance().andReturn().header(CorrelationIdFilter.HEADER)
        val badRequest = requestBalance("abc").andReturn().header(CorrelationIdFilter.HEADER)

        assertTrue(!notFound.isNullOrBlank() && !badRequest.isNullOrBlank())
        assertNotEquals(notFound, badRequest)
    }

    @Test
    fun `the logging context is cleaned after a successful request`() {
        doReturn(snapshot).`when`(getBalance).getBalance(accountId)

        mockMvc.perform(get("/balances/{id}", DEFAULT_ACCOUNT_ID).header(CorrelationIdFilter.HEADER, "abc"))

        assertNull(MDC.get("correlationId"))
        assertNull(MDC.get("accountId"))
    }

    @Test
    fun `the logging context is cleaned after a failed request`() {
        doThrow(RuntimeException("x")).`when`(getBalance).getBalance(accountId)

        requestBalance()

        assertNull(MDC.get("correlationId"))
        assertNull(MDC.get("accountId"))
    }

    @Test
    fun `the error logged by the advice still carries the account id and the correlation id`() {
        doThrow(RuntimeException("x")).`when`(getBalance).getBalance(accountId)

        mockMvc.perform(get("/balances/{id}", DEFAULT_ACCOUNT_ID).header(CorrelationIdFilter.HEADER, "abc-1"))

        val line = advice.events.single()
        assertEquals(DEFAULT_ACCOUNT_ID, line.mdcPropertyMap["accountId"])
        assertEquals("abc-1", line.mdcPropertyMap["correlationId"])
    }

    @Test
    fun `an unknown route is answered as problem json with a correlation id`() {
        mockMvc
            .perform(get("/nao-existe"))
            .andExpect(status().isNotFound)
            .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
            .andExpect(header().exists(CorrelationIdFilter.HEADER))
    }

    @Test
    fun `an unsupported method is answered as problem json`() {
        mockMvc
            .perform(post("/balances/{id}", DEFAULT_ACCOUNT_ID))
            .andExpect(status().isMethodNotAllowed)
            .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
    }

    private companion object {
        val LOWERCASE_UUID = Regex("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    }
}
