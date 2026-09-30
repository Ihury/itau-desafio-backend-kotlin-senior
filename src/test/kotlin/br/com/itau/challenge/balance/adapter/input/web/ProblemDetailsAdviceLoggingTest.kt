package br.com.itau.challenge.balance.adapter.input.web

import br.com.itau.challenge.balance.domain.exception.BalanceStoreCircuitOpenException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.domain.model.StoreFailureDetails
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.mock.web.MockHttpServletRequest
import software.amazon.awssdk.core.exception.SdkClientException
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Logs do `503` da consulta: diagnostico do SDK sem texto livre, sem pilha e no nivel certo. */
class ProblemDetailsAdviceLoggingTest {
    private val advice = ProblemDetailsAdvice(Duration.ofSeconds(10))
    private val request = MockHttpServletRequest("GET", "/balances/5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")

    private val logger = LoggerFactory.getLogger(ProblemDetailsAdvice::class.java) as Logger
    private lateinit var appender: ListAppender<ILoggingEvent>
    private var originalLevel: Level? = null

    @BeforeEach
    fun captureLogs() {
        originalLevel = logger.level
        appender = ListAppender<ILoggingEvent>().apply { start() }
        logger.addAppender(appender)
        logger.level = Level.DEBUG
    }

    @AfterEach
    fun releaseLogs() {
        logger.detachAppender(appender)
        logger.level = originalLevel
    }

    private fun storeUnavailable(
        cause: StoreFailureCause,
        details: StoreFailureDetails?,
    ) = BalanceStoreUnavailableException(cause, SdkClientException.builder().message("segredo do sdk").build(), details)

    @Test
    fun `an unavailable store answers 503 and logs one warn with the cause and the sdk diagnostics, without free text or stack`() {
        val response = advice.storeUnavailable(storeUnavailable(StoreFailureCause.UNAVAILABLE, StoreFailureDetails("software.amazon.awssdk.core.exception.SdkClientException", null, null)), request)

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.statusCode)
        val line = appender.list.single()
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
            assertEquals("10", response.headers.getFirst("Retry-After"))
        }

        assertTrue(appender.list.none { it.level.isGreaterOrEqual(Level.INFO) }, "nenhum INFO/WARN/ERROR: ${appender.list.map { it.formattedMessage }}")
        assertTrue(appender.list.all { it.level == Level.DEBUG && it.throwableProxy == null })
    }

    @Test
    fun `fifty real failures still log fifty warns because each one is a real read failure`() {
        repeat(50) { advice.storeUnavailable(storeUnavailable(StoreFailureCause.TIMEOUT, StoreFailureDetails("software.amazon.awssdk.core.exception.ApiCallTimeoutException")), request) }

        assertEquals(50, appender.list.count { it.level == Level.WARN })
    }

    @Test
    fun `a misconfigured store logs at error with the error code and the status code`() {
        advice.storeUnavailable(
            storeUnavailable(StoreFailureCause.MISCONFIGURED, StoreFailureDetails("software.amazon.awssdk.services.dynamodb.model.DynamoDbException", "UnrecognizedClientException", 400)),
            request,
        )

        val line = appender.list.single()
        assertEquals(Level.ERROR, line.level)
        assertTrue("cause=MISCONFIGURED" in line.formattedMessage, line.formattedMessage)
        assertTrue("errorCode=UnrecognizedClientException" in line.formattedMessage && "statusCode=400" in line.formattedMessage, line.formattedMessage)
        assertNull(line.throwableProxy, "sem pilha")
    }
}
