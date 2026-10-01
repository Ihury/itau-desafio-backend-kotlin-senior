package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.domain.model.StoreFailureDetails
import br.com.itau.challenge.balance.testing.DeadLetterHarness
import br.com.itau.challenge.balance.testing.LogCapture
import ch.qos.logback.classic.Level
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import org.slf4j.MDC
import software.amazon.awssdk.core.exception.SdkClientException
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DeadLetterLoggingTest {
    private val dlt = DeadLetterHarness()

    @JvmField
    @RegisterExtension
    val logs = LogCapture(DeadLetterRetryListener::class.java)

    private fun storeFailure(
        cause: StoreFailureCause,
        details: StoreFailureDetails?,
    ) = BalanceStoreUnavailableException(cause, SdkClientException.builder().message("segredo do sdk: 315e3cfe-f4af-4cd2-b298-a449e614349a").build(), details)

    @Test
    fun `an invalid event is logged once as an isolation with the reason and never as an unclassified failure`() {
        dlt.deliver(InvalidEventException(RejectionReason.INVALID_CURRENCY, "transaction.currency"))

        assertTrue(logs.at(Level.ERROR).isEmpty(), "evento invalido e um desfecho esperado, nao um defeito: ${logs.messages}")
        val isolated = logs.messageStartingWith("message isolated in the dlt")
        assertEquals(Level.WARN, isolated.level)
        assertTrue("reason=invalid_currency" in isolated.formattedMessage && "detail=transaction.currency" in isolated.formattedMessage)
    }

    @Test
    fun `a transient failure logs at warn the cause and the sdk diagnostics, never the free message, the payload or the stack`() {
        dlt.deliver(storeFailure(StoreFailureCause.THROTTLED, StoreFailureDetails("software.amazon.awssdk.services.dynamodb.model.DynamoDbException", "ThrottlingException", 400)))

        val line = logs.messageStartingWith("store unavailable")
        assertEquals(Level.WARN, line.level)
        assertTrue("cause=THROTTLED" in line.formattedMessage, line.formattedMessage)
        assertTrue("exception=software.amazon.awssdk.services.dynamodb.model.DynamoDbException" in line.formattedMessage, line.formattedMessage)
        assertTrue("errorCode=ThrottlingException" in line.formattedMessage && "statusCode=400" in line.formattedMessage, line.formattedMessage)
        assertFalse("segredo" in line.formattedMessage || "315e3cfe" in line.formattedMessage, "sem a mensagem livre do SDK")
        assertNull(line.throwableProxy, "sem pilha")
    }

    @Test
    fun `a misconfigured store logs at error, still transient and never sent to the dlt`() {
        val recovered = dlt.deliver(storeFailure(StoreFailureCause.MISCONFIGURED, StoreFailureDetails("software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException", "ResourceNotFoundException", 400)))

        assertFalse(recovered, "a falha de configuracao continua sendo retentada")
        assertTrue(dlt.publications.isEmpty(), "nunca vai ao DLT")
        val line = logs.messageStartingWith("store unavailable")
        assertEquals(Level.ERROR, line.level)
        assertTrue("cause=MISCONFIGURED" in line.formattedMessage && "errorCode=ResourceNotFoundException" in line.formattedMessage, line.formattedMessage)
        assertNull(line.throwableProxy, "sem pilha")
    }

    @Test
    fun `a transient failure without sdk diagnostics still logs the cause`() {
        dlt.deliver(BalanceStoreUnavailableException(StoreFailureCause.UNAVAILABLE))

        val line = logs.messageStartingWith("store unavailable")
        assertEquals(Level.WARN, line.level)
        assertTrue("cause=UNAVAILABLE" in line.formattedMessage, line.formattedMessage)
    }

    @Test
    fun `the failure cycle logs carry the topic partition offset as correlation id and clean the mdc afterwards`() {
        dlt.deliver(IllegalStateException("defeito"))
        dlt.deliver(BalanceStoreUnavailableException(StoreFailureCause.UNAVAILABLE))
        dlt.deliver(InvalidEventException(RejectionReason.MISSING_FIELD))

        val correlated = logs.events.filter { it.loggerName == DeadLetterRetryListener::class.java.name }
        assertTrue(correlated.size >= 3)
        correlated.forEach {
            assertEquals("transacoes-financeiras-processadas-7@41", it.mdcPropertyMap["correlationId"], it.formattedMessage)
        }
        assertNull(MDC.get("correlationId"), "o MDC do thread do consumer e sempre limpo")
    }
}
