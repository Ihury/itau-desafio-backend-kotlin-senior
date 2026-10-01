package br.com.itau.challenge.balance.adapter.input.web

import br.com.itau.challenge.balance.domain.exception.BalanceStoreCircuitOpenException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.domain.model.StoreFailureDetails
import br.com.itau.challenge.balance.testing.LogCapture
import br.com.itau.challenge.balance.testing.RETRY_AFTER
import br.com.itau.challenge.balance.testing.RETRY_AFTER_SECONDS
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_ACCOUNT_ID
import ch.qos.logback.classic.Level
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import org.springframework.http.HttpStatus
import org.springframework.mock.web.MockHttpServletRequest
import software.amazon.awssdk.core.exception.SdkClientException
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ProblemDetailsAdviceLoggingTest {
    private val advice = ProblemDetailsAdvice(RETRY_AFTER)
    private val request = MockHttpServletRequest("GET", "/balances/$DEFAULT_ACCOUNT_ID")

    @JvmField
    @RegisterExtension
    val logs = LogCapture(ProblemDetailsAdvice::class.java, Level.DEBUG)

    private fun storeUnavailable(
        cause: StoreFailureCause,
        details: StoreFailureDetails?,
    ) = BalanceStoreUnavailableException(cause, SdkClientException.builder().message("segredo do sdk").build(), details)

    @Test
    fun `an unavailable store answers 503 and logs one warn with the cause and the sdk diagnostics, without free text or stack`() {
        val response = advice.storeUnavailable(storeUnavailable(StoreFailureCause.UNAVAILABLE, StoreFailureDetails("software.amazon.awssdk.core.exception.SdkClientException", null, null)), request)

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.statusCode)
        val line = logs.events.single()
        assertEquals(Level.WARN, line.level)
        assertTrue("cause=UNAVAILABLE" in line.formattedMessage, line.formattedMessage)
        assertTrue("exception=software.amazon.awssdk.core.exception.SdkClientException" in line.formattedMessage, line.formattedMessage)
        assertFalse("segredo" in line.formattedMessage)
        assertNull(line.throwableProxy, "sem pilha")
    }

    @Test
    fun `fifty rejections by the open circuit answer 503 with retry after and log no warn or error, only debug and without stack`() {
        val rejection = BalanceStoreCircuitOpenException(SdkClientException.builder().message("segredo").build())

        repeat(50) {
            val response = advice.storeUnavailable(rejection, request)
            assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.statusCode)
            assertEquals(RETRY_AFTER_SECONDS, response.headers.getFirst("Retry-After"))
        }

        assertTrue(logs.events.none { it.level.isGreaterOrEqual(Level.INFO) }, "nenhum INFO/WARN/ERROR: ${logs.messages}")
        assertTrue(logs.events.all { it.level == Level.DEBUG && it.throwableProxy == null })
    }

    @Test
    fun `fifty real failures still log fifty warns because each one is a real read failure`() {
        repeat(50) { advice.storeUnavailable(storeUnavailable(StoreFailureCause.TIMEOUT, StoreFailureDetails("software.amazon.awssdk.core.exception.ApiCallTimeoutException")), request) }

        assertEquals(50, logs.at(Level.WARN).size)
    }

    @Test
    fun `a misconfigured store logs at error with the error code and the status code`() {
        advice.storeUnavailable(
            storeUnavailable(StoreFailureCause.MISCONFIGURED, StoreFailureDetails("software.amazon.awssdk.services.dynamodb.model.DynamoDbException", "UnrecognizedClientException", 400)),
            request,
        )

        val line = logs.events.single()
        assertEquals(Level.ERROR, line.level)
        assertTrue("cause=MISCONFIGURED" in line.formattedMessage, line.formattedMessage)
        assertTrue("errorCode=UnrecognizedClientException" in line.formattedMessage && "statusCode=400" in line.formattedMessage, line.formattedMessage)
        assertNull(line.throwableProxy, "sem pilha")
    }
}
