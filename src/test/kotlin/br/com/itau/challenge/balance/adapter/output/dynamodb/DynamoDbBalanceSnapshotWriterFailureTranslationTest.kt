package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.exception.BalanceStoreRejectedException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.testing.serviceError
import org.junit.jupiter.api.Test
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException
import software.amazon.awssdk.core.exception.ApiCallTimeoutException
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class DynamoDbBalanceSnapshotWriterFailureTranslationTest : DynamoDbWriterFixture() {
    @Test
    fun `each sdk failure becomes its store failure cause and keeps the original as cause`() {
        val transientFailures =
            mapOf(
                ProvisionedThroughputExceededException.builder().message("x").build() to StoreFailureCause.THROTTLED,
                serviceError(400, "ThrottlingException") to StoreFailureCause.THROTTLED,
                ApiCallTimeoutException.builder().message("x").build() to StoreFailureCause.TIMEOUT,
                ApiCallAttemptTimeoutException.builder().message("x").build() to StoreFailureCause.TIMEOUT,
                SdkClientException.builder().message("Unable to execute HTTP request").build() to StoreFailureCause.UNAVAILABLE,
                InternalServerErrorException.builder().message("x").build() to StoreFailureCause.UNAVAILABLE,
                serviceError(503, "ServiceUnavailable") to StoreFailureCause.UNAVAILABLE,
            )
        transientFailures.forEach { (failure, expected) ->
            stubUpdateFailsWith(failure)

            val thrown = assertFailsWith<BalanceStoreUnavailableException> { writer.applyIfNewer(snapshot) }

            assertEquals(expected, thrown.failureCause, failure.javaClass.simpleName)
            assertSame(failure, thrown.cause)
        }
    }

    @Test
    fun `a validation exception is a rejection of the store`() {
        val validation = serviceError(400, "ValidationException")
        stubUpdateFailsWith(validation)

        val thrown = assertFailsWith<BalanceStoreRejectedException> { writer.applyIfNewer(snapshot) }

        assertSame(validation, thrown.cause)
    }

    @Test
    fun `failures that are not from the sdk propagate as they are`() {
        val defect = IllegalStateException("defeito")
        stubUpdateFailsWith(defect)

        assertSame(defect, assertFailsWith<IllegalStateException> { writer.applyIfNewer(snapshot) })
    }

    @Test
    fun `translated failures never carry the payload of the event`() {
        stubUpdateFailsWith(serviceError(500, "InternalFailure"))

        val thrown = assertFailsWith<BalanceStoreUnavailableException> { writer.applyIfNewer(snapshot) }

        assertTrue("183.12" !in thrown.message.orEmpty() && "315e3cfe" !in thrown.message.orEmpty())
        assertNull(thrown.cause?.cause)
    }
}
