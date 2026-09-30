package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.exception.BalanceStoreRejectedException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.domain.model.StoreFailureDetails
import org.junit.jupiter.api.Test
import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException
import software.amazon.awssdk.core.exception.ApiCallTimeoutException
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException
import software.amazon.awssdk.services.dynamodb.model.RequestLimitExceededException
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame

class DynamoDbExceptionTranslatorTest {
    private fun serviceError(
        status: Int,
        code: String,
    ): DynamoDbException =
        DynamoDbException
            .builder()
            .statusCode(status)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode(code).errorMessage("detalhe interno").build())
            .message("detalhe interno")
            .build() as DynamoDbException

    private fun readCause(failure: Throwable): StoreFailureCause {
        val translated = DynamoDbExceptionTranslator.forRead(failure)
        assertIs<BalanceStoreUnavailableException>(translated)
        assertSame(failure, translated.cause, "a excecao original do SDK e preservada como cause")
        return translated.failureCause
    }

    @Test
    fun `throttling failures are classified as throttled on read`() {
        assertEquals(StoreFailureCause.THROTTLED, readCause(ProvisionedThroughputExceededException.builder().message("x").build()))
        assertEquals(StoreFailureCause.THROTTLED, readCause(RequestLimitExceededException.builder().message("x").build()))
        assertEquals(StoreFailureCause.THROTTLED, readCause(serviceError(400, "ThrottlingException")))
    }

    @Test
    fun `client side timeouts are classified as timeout`() {
        assertEquals(StoreFailureCause.TIMEOUT, readCause(ApiCallTimeoutException.builder().message("x").build()))
        assertEquals(StoreFailureCause.TIMEOUT, readCause(ApiCallAttemptTimeoutException.builder().message("x").build()))
    }

    @Test
    fun `server errors and connection failures are unavailable`() {
        assertEquals(StoreFailureCause.UNAVAILABLE, readCause(serviceError(500, "InternalFailure")))
        assertEquals(StoreFailureCause.UNAVAILABLE, readCause(serviceError(503, "ServiceUnavailable")))
        assertEquals(StoreFailureCause.UNAVAILABLE, readCause(InternalServerErrorException.builder().message("x").build()))
        assertEquals(StoreFailureCause.UNAVAILABLE, readCause(SdkClientException.builder().message("Unable to execute HTTP request").build()))
    }

    private fun credentialsFailure() =
        SdkClientException
            .builder()
            .message("Unable to load credentials from any of the providers in the chain AwsCredentialsProviderChain: [...]")
            .build()

    @Test
    fun `missing table, denied access and credential problems are misconfigured on read`() {
        assertEquals(StoreFailureCause.MISCONFIGURED, readCause(ResourceNotFoundException.builder().message("x").build()))
        listOf(
            "ResourceNotFoundException",
            "AccessDeniedException",
            "UnrecognizedClientException",
            "ExpiredTokenException",
            "ExpiredToken",
            "InvalidSignatureException",
            "MissingAuthenticationToken",
            "MissingAuthenticationTokenException",
            "com.amazon.coral.service#UnrecognizedClientException",
        ).forEach { code -> assertEquals(StoreFailureCause.MISCONFIGURED, readCause(serviceError(400, code)), code) }
        assertEquals(StoreFailureCause.MISCONFIGURED, readCause(credentialsFailure()))
        assertEquals(
            StoreFailureCause.MISCONFIGURED,
            readCause(SdkClientException.builder().message("Unable to execute HTTP request").cause(credentialsFailure()).build()),
            "a falha de credencial aninhada tambem e reconhecida",
        )
    }

    @Test
    fun `misconfiguration stays a transitory failure on write, never a rejection`() {
        listOf<Throwable>(
            ResourceNotFoundException.builder().message("x").build(),
            serviceError(400, "AccessDeniedException"),
            serviceError(400, "ExpiredTokenException"),
            credentialsFailure(),
        ).forEach { failure ->
            val translated = DynamoDbExceptionTranslator.forWrite(failure)
            assertIs<BalanceStoreUnavailableException>(translated)
            assertEquals(StoreFailureCause.MISCONFIGURED, translated.failureCause)
            assertSame(failure, translated.cause)
        }
    }

    private fun detailsOf(failure: Throwable): StoreFailureDetails {
        val translated = DynamoDbExceptionTranslator.forRead(failure)
        assertIs<BalanceStoreUnavailableException>(translated)
        return assertNotNull(translated.details)
    }

    @Test
    fun `the diagnostic details carry the sdk exception class, the error code and the status code`() {
        val details = detailsOf(serviceError(400, "com.amazon.coral.service#UnrecognizedClientException"))

        assertEquals("software.amazon.awssdk.services.dynamodb.model.DynamoDbException", details.exceptionClass)
        assertEquals("UnrecognizedClientException", details.errorCode)
        assertEquals(400, details.statusCode)
        assertEquals(
            "exception=software.amazon.awssdk.services.dynamodb.model.DynamoDbException errorCode=UnrecognizedClientException statusCode=400",
            details.toString(),
        )
    }

    @Test
    fun `the diagnostic details omit what does not exist and never carry the free text of the sdk`() {
        val client = detailsOf(SdkClientException.builder().message("segredo do sdk").build())
        assertEquals("software.amazon.awssdk.core.exception.SdkClientException", client.exceptionClass)
        assertNull(client.errorCode)
        assertNull(client.statusCode)
        assertEquals("exception=software.amazon.awssdk.core.exception.SdkClientException", client.toString())

        val withoutDetails = detailsOf(AwsServiceException.builder().statusCode(418).message("segredo do sdk").build())
        assertNull(withoutDetails.errorCode)
        assertEquals(418, withoutDetails.statusCode)

        // errorCode vem do servidor: so um token curto e seguro vai ao log (nada de quebra de linha nem de texto livre)
        assertNull(detailsOf(serviceError(400, "linha 1\nlinha 2 com espacos")).errorCode)
        assertNull(detailsOf(serviceError(400, "x".repeat(200))).errorCode)

        listOf(client, withoutDetails, detailsOf(serviceError(400, "ThrottlingException"))).forEach {
            assertFalse("segredo" in it.toString() || "detalhe interno" in it.toString(), it.toString())
        }
    }

    @Test
    fun `validation failure on read is unavailable and never rejected`() {
        assertEquals(StoreFailureCause.UNAVAILABLE, readCause(serviceError(400, "ValidationException")))
    }

    @Test
    fun `validation failure on write is a rejection and other failures stay transitory`() {
        val validation = serviceError(400, "ValidationException")
        val rejected = DynamoDbExceptionTranslator.forWrite(validation)
        assertIs<BalanceStoreRejectedException>(rejected)
        assertSame(validation, rejected.cause)

        val namespaced = serviceError(400, "com.amazon.coral.validate#ValidationException")
        assertIs<BalanceStoreRejectedException>(DynamoDbExceptionTranslator.forWrite(namespaced))

        val throttled = DynamoDbExceptionTranslator.forWrite(ProvisionedThroughputExceededException.builder().message("x").build())
        assertIs<BalanceStoreUnavailableException>(throttled)
        assertEquals(StoreFailureCause.THROTTLED, throttled.failureCause)

        val serverError = DynamoDbExceptionTranslator.forWrite(serviceError(500, "InternalFailure"))
        assertIs<BalanceStoreUnavailableException>(serverError)
        assertEquals(StoreFailureCause.UNAVAILABLE, serverError.failureCause)

        val timeout = DynamoDbExceptionTranslator.forWrite(ApiCallTimeoutException.builder().message("x").build())
        assertIs<BalanceStoreUnavailableException>(timeout)
        assertEquals(StoreFailureCause.TIMEOUT, timeout.failureCause)
    }

    @Test
    fun `an exception that does not come from the sdk is not translated`() {
        assertNull(DynamoDbExceptionTranslator.forRead(IllegalStateException("x")))
        assertNull(DynamoDbExceptionTranslator.forWrite(RuntimeException("x")))
    }

    @Test
    fun `any other sdk exception is treated as unavailable`() {
        val unknown = AwsServiceException.builder().statusCode(418).message("x").build()
        assertEquals(StoreFailureCause.UNAVAILABLE, readCause(unknown))
    }
}
