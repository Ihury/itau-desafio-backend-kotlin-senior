package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.exception.BalanceStoreRejectedException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
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
    fun `server errors, connection failures, missing table and credential errors are unavailable`() {
        assertEquals(StoreFailureCause.UNAVAILABLE, readCause(serviceError(500, "InternalFailure")))
        assertEquals(StoreFailureCause.UNAVAILABLE, readCause(serviceError(503, "ServiceUnavailable")))
        assertEquals(StoreFailureCause.UNAVAILABLE, readCause(InternalServerErrorException.builder().message("x").build()))
        assertEquals(StoreFailureCause.UNAVAILABLE, readCause(SdkClientException.builder().message("Unable to execute HTTP request").build()))
        assertEquals(StoreFailureCause.UNAVAILABLE, readCause(ResourceNotFoundException.builder().message("x").build()))
        assertEquals(StoreFailureCause.UNAVAILABLE, readCause(serviceError(400, "UnrecognizedClientException")))
        assertEquals(StoreFailureCause.UNAVAILABLE, readCause(serviceError(400, "AccessDeniedException")))
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

        val missing = DynamoDbExceptionTranslator.forWrite(ResourceNotFoundException.builder().message("x").build())
        assertIs<BalanceStoreUnavailableException>(missing)
        assertEquals(StoreFailureCause.UNAVAILABLE, missing.failureCause)

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
