package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.exception.BalanceStoreRejectedException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.domain.model.TransactionEventFixtures.transactionEvent
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException
import software.amazon.awssdk.core.exception.ApiCallTimeoutException
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class DynamoDbBalanceSnapshotWriterTest {
    private val client = mock(DynamoDbClient::class.java)
    private val writer = DynamoDbBalanceSnapshotWriter(client, "AccountBalances")
    private val snapshot = BalanceSnapshot.from(transactionEvent())

    private fun succeed() {
        doReturn(UpdateItemResponse.builder().build()).`when`(client).updateItem(any(UpdateItemRequest::class.java))
    }

    private fun failWith(failure: Throwable) {
        doThrow(failure).`when`(client).updateItem(any(UpdateItemRequest::class.java))
    }

    private fun capturedRequest(): UpdateItemRequest {
        val captor = ArgumentCaptor.forClass(UpdateItemRequest::class.java)
        verify(client, times(1)).updateItem(captor.capture())
        return captor.value
    }

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

    @Test
    fun `a single conditional update item with the exact expressions is sent`() {
        succeed()

        writer.applyIfNewer(snapshot)

        val request = capturedRequest()
        assertEquals("AccountBalances", request.tableName())
        assertEquals(BalanceItemMapper.keyOf(snapshot.accountId), request.key())
        assertEquals(
            "SET schemaVersion = :v, ownerId = :o, accountStatus = :st, balanceAmount = :amt, balanceCurrency = :cur, " +
                "accountCreatedAtMicros = :cr, lastTxTsMicros = :ts, lastTxId = :tx",
            request.updateExpression(),
        )
        assertEquals(
            "attribute_not_exists(pk) OR lastTxTsMicros < :ts OR (lastTxTsMicros = :ts AND lastTxId < :tx)",
            request.conditionExpression(),
        )
        assertEquals(ReturnValuesOnConditionCheckFailure.ALL_OLD, request.returnValuesOnConditionCheckFailure())
    }

    @Test
    fun `values carry the snapshot with numbers as N and the transaction id in lower case`() {
        succeed()
        val upper = BalanceSnapshot.from(transactionEvent(transactionId = "8E8AE808-B154-48B5-9F3E-553935CC4543", timestampMicros = 1751749453433123L))

        writer.applyIfNewer(upper)

        val values = capturedRequest().expressionAttributeValues()
        assertEquals(
            setOf(":v", ":o", ":st", ":amt", ":cur", ":cr", ":ts", ":tx"),
            values.keys,
        )
        assertEquals(AttributeValue.builder().n("1").build(), values[":v"])
        assertEquals(AttributeValue.builder().s("315e3cfe-f4af-4cd2-b298-a449e614349a").build(), values[":o"])
        assertEquals(AttributeValue.builder().s("ENABLED").build(), values[":st"])
        assertEquals(AttributeValue.builder().n("183.12").build(), values[":amt"])
        assertEquals(AttributeValue.builder().s("BRL").build(), values[":cur"])
        assertEquals(AttributeValue.builder().n("1634874339000000").build(), values[":cr"])
        assertEquals(AttributeValue.builder().n("1751749453433123").build(), values[":ts"])
        assertEquals(AttributeValue.builder().s("8e8ae808-b154-48b5-9f3e-553935cc4543").build(), values[":tx"])
    }

    @Test
    fun `the balance is written as a plain decimal never in scientific notation`() {
        succeed()

        writer.applyIfNewer(BalanceSnapshot.from(transactionEvent(balanceAmount = "0.0000001")))
        val small = capturedRequest().expressionAttributeValues().getValue(":amt")

        assertEquals("0.0000001", small.n())
    }

    @Test
    fun `a successful update is applied`() {
        succeed()

        assertEquals(ApplyResult.Applied, writer.applyIfNewer(snapshot))
    }

    @Test
    fun `a failed condition is obsolete and never an error`() {
        failWith(ConditionalCheckFailedException.builder().message("The conditional request failed").build())

        assertEquals(ApplyResult.Obsolete, writer.applyIfNewer(snapshot))
    }

    @Test
    fun `there is no read before or after the write, so no read-modify-write`() {
        succeed()
        writer.applyIfNewer(snapshot)
        failWith(ConditionalCheckFailedException.builder().message("x").build())
        writer.applyIfNewer(snapshot)

        verify(client, never()).getItem(any(GetItemRequest::class.java))
        verify(client, times(2)).updateItem(any(UpdateItemRequest::class.java))
    }

    @Test
    fun `sdk failures are translated by the matrix and never swallowed`() {
        val transitory =
            mapOf(
                ProvisionedThroughputExceededException.builder().message("x").build() to StoreFailureCause.THROTTLED,
                serviceError(400, "ThrottlingException") to StoreFailureCause.THROTTLED,
                ApiCallTimeoutException.builder().message("x").build() to StoreFailureCause.TIMEOUT,
                ApiCallAttemptTimeoutException.builder().message("x").build() to StoreFailureCause.TIMEOUT,
                SdkClientException.builder().message("Unable to execute HTTP request").build() to StoreFailureCause.UNAVAILABLE,
                InternalServerErrorException.builder().message("x").build() to StoreFailureCause.UNAVAILABLE,
                serviceError(503, "ServiceUnavailable") to StoreFailureCause.UNAVAILABLE,
            )
        transitory.forEach { (failure, expected) ->
            failWith(failure)

            val thrown = assertFailsWith<BalanceStoreUnavailableException> { writer.applyIfNewer(snapshot) }

            assertEquals(expected, thrown.failureCause, failure.javaClass.simpleName)
            assertSame(failure, thrown.cause)
        }
    }

    @Test
    fun `a validation exception is a rejection of the store`() {
        val validation = serviceError(400, "ValidationException")
        failWith(validation)

        val thrown = assertFailsWith<BalanceStoreRejectedException> { writer.applyIfNewer(snapshot) }

        assertSame(validation, thrown.cause)
    }

    @Test
    fun `failures that are not from the sdk propagate as they are`() {
        val defect = IllegalStateException("defeito")
        failWith(defect)

        assertSame(defect, assertFailsWith<IllegalStateException> { writer.applyIfNewer(snapshot) })
    }

    @Test
    fun `translated failures never carry the payload of the event`() {
        failWith(serviceError(500, "InternalFailure"))

        val thrown = assertFailsWith<BalanceStoreUnavailableException> { writer.applyIfNewer(snapshot) }

        assertTrue("183.12" !in thrown.message.orEmpty() && "315e3cfe" !in thrown.message.orEmpty())
        assertNull(thrown.cause?.cause)
    }
}
