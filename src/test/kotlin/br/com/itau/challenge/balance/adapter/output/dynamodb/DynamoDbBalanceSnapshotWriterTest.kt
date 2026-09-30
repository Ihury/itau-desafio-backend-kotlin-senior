package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.exception.BalanceStoreRejectedException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.AccountStatus
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.domain.model.TransactionEvent
import br.com.itau.challenge.balance.domain.model.TransactionEventFixtures.transactionEvent
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
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
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
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
    private val registry = SimpleMeterRegistry()
    private val writer = DynamoDbBalanceSnapshotWriter(client, "AccountBalances", registry)
    private val snapshot = BalanceSnapshot.from(transactionEvent())

    private fun succeed() {
        doReturn(UpdateItemResponse.builder().build()).`when`(client).updateItem(any(UpdateItemRequest::class.java))
    }

    private fun failWith(failure: Throwable) {
        doThrow(failure).`when`(client).updateItem(any(UpdateItemRequest::class.java))
    }

    private fun itemOf(event: TransactionEvent) = BalanceItemMapper.toItem(BalanceSnapshot.from(event))

    /** Falha a condicao devolvendo o item vigente (`ALL_OLD`). */
    private fun failConditionWith(current: Map<String, AttributeValue>) {
        failWith(ConditionalCheckFailedException.builder().message("The conditional request failed").item(current).build())
    }

    private fun failConditionWithoutItem() {
        failWith(ConditionalCheckFailedException.builder().message("The conditional request failed").build())
    }

    private fun stubGetItem(item: Map<String, AttributeValue>?) {
        val response = if (item == null) GetItemResponse.builder().build() else GetItemResponse.builder().item(item).build()
        doReturn(response).`when`(client).getItem(any(GetItemRequest::class.java))
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
    fun `a failed condition against a greater current item is obsolete and never an error`() {
        failConditionWith(itemOf(transactionEvent(timestampMicros = snapshot.precedence.timestamp.micros + 1, balanceAmount = "999.00")))

        assertEquals(ApplyResult.Obsolete, writer.applyIfNewer(snapshot))
    }

    @Test
    fun `a failed condition against the same timestamp and a greater transaction id is obsolete`() {
        failConditionWith(itemOf(transactionEvent(transactionId = "ffffffff-ffff-4fff-8fff-ffffffffff01")))

        assertEquals(ApplyResult.Obsolete, writer.applyIfNewer(snapshot))
    }

    @Test
    fun `a failed condition against the same key and content is a plain duplicate`() {
        failConditionWith(itemOf(transactionEvent()))

        assertEquals(ApplyResult.Duplicate(conflicting = false), writer.applyIfNewer(snapshot))
    }

    @Test
    fun `a duplicate whose balance differs only by scale is not conflicting`() {
        val stored = itemOf(transactionEvent()).toMutableMap()
        stored["balanceAmount"] = AttributeValue.builder().n("183.120").build()
        failConditionWith(stored)

        assertEquals(ApplyResult.Duplicate(conflicting = false), writer.applyIfNewer(snapshot))

        val normalized = itemOf(transactionEvent(balanceAmount = "183.10")).toMutableMap()
        normalized["balanceAmount"] = AttributeValue.builder().n("183.1").build()
        failConditionWith(normalized)
        assertEquals(ApplyResult.Duplicate(conflicting = false), writer.applyIfNewer(BalanceSnapshot.from(transactionEvent(balanceAmount = "183.10"))))
    }

    @Test
    fun `a duplicate with divergent content is a conflicting duplicate for each compared field`() {
        val divergent =
            mapOf(
                "ownerId" to transactionEvent(ownerId = "dddddddd-f4af-4cd2-b298-a449e614349a"),
                "accountStatus" to transactionEvent(accountStatus = AccountStatus.DISABLED),
                "balanceCurrency" to transactionEvent(balanceCurrency = "USD"),
                "balanceAmount" to transactionEvent(balanceAmount = "999.99"),
            )
        divergent.forEach { (field, event) ->
            failConditionWith(itemOf(event))

            assertEquals(ApplyResult.Duplicate(conflicting = true), writer.applyIfNewer(snapshot), field)
        }
    }

    @Test
    fun `there is no read before the write and none when the failed condition carries the old item`() {
        succeed()
        writer.applyIfNewer(snapshot)
        failConditionWith(itemOf(transactionEvent()))
        writer.applyIfNewer(snapshot)

        verify(client, never()).getItem(any(GetItemRequest::class.java))
        verify(client, times(2)).updateItem(any(UpdateItemRequest::class.java))
    }

    @Test
    fun `without the old item a single consistent get item classifies the outcome`() {
        failConditionWithoutItem()
        stubGetItem(itemOf(transactionEvent(timestampMicros = snapshot.precedence.timestamp.micros + 5)))

        assertEquals(ApplyResult.Obsolete, writer.applyIfNewer(snapshot))

        val captor = ArgumentCaptor.forClass(GetItemRequest::class.java)
        verify(client, times(1)).getItem(captor.capture())
        assertEquals(true, captor.value.consistentRead())
        assertEquals("AccountBalances", captor.value.tableName())
        assertEquals(BalanceItemMapper.keyOf(snapshot.accountId), captor.value.key())
    }

    @Test
    fun `the fallback read also distinguishes duplicate and conflicting duplicate`() {
        failConditionWithoutItem()
        stubGetItem(itemOf(transactionEvent()))
        assertEquals(ApplyResult.Duplicate(conflicting = false), writer.applyIfNewer(snapshot))

        stubGetItem(itemOf(transactionEvent(balanceAmount = "1.00")))
        assertEquals(ApplyResult.Duplicate(conflicting = true), writer.applyIfNewer(snapshot))
    }

    @Test
    fun `when the item is missing even in the fallback read the failure is transitory and the event is redelivered`() {
        failConditionWithoutItem()
        stubGetItem(null)

        val thrown = assertFailsWith<BalanceStoreUnavailableException> { writer.applyIfNewer(snapshot) }

        assertEquals(StoreFailureCause.UNAVAILABLE, thrown.failureCause)
    }

    @Test
    fun `a current item with a lower precedence than the event contradicts the failed condition and is not transitory`() {
        failConditionWithoutItem()
        stubGetItem(itemOf(transactionEvent(timestampMicros = snapshot.precedence.timestamp.micros - 1, balanceAmount = "5555.55")))

        val thrown = assertFailsWith<IllegalStateException> { writer.applyIfNewer(snapshot) }

        assertTrue(thrown !is BalanceStoreUnavailableException, "retentar para sempre bloquearia a particao")
        assertTrue("5555.55" !in thrown.message.orEmpty() && "183.12" !in thrown.message.orEmpty(), "sem valores na mensagem")
    }

    @Test
    fun `the same contradiction carried by the old item of the failed condition is also an internal failure`() {
        failConditionWith(itemOf(transactionEvent(timestampMicros = snapshot.precedence.timestamp.micros - 1, balanceAmount = "5555.55")))

        val thrown = assertFailsWith<IllegalStateException> { writer.applyIfNewer(snapshot) }

        assertTrue("5555.55" !in thrown.message.orEmpty(), "sem valores na mensagem")
        verify(client, never()).getItem(any(GetItemRequest::class.java))
    }

    @Test
    fun `a failure of the fallback read is translated and never swallowed`() {
        failConditionWithoutItem()
        doThrow(ProvisionedThroughputExceededException.builder().message("x").build()).`when`(client).getItem(any(GetItemRequest::class.java))

        val thrown = assertFailsWith<BalanceStoreUnavailableException> { writer.applyIfNewer(snapshot) }

        assertEquals(StoreFailureCause.THROTTLED, thrown.failureCause)
    }

    @Test
    fun `a current item without the precedence attributes is an internal failure and never a classification`() {
        failConditionWith(mapOf("pk" to AttributeValue.builder().s("ACCOUNT#x").build()))

        assertFailsWith<IllegalStateException> { writer.applyIfNewer(snapshot) }
    }

    @Test
    fun `an unreadable current item fails without leaking balances or owners in the message`() {
        failConditionWith(mapOf("lastTxTsMicros" to AttributeValue.builder().n("abc").build()))

        val thrown = assertFailsWith<IllegalStateException> { writer.applyIfNewer(snapshot) }

        assertTrue("183.12" !in thrown.message.orEmpty() && "315e3cfe" !in thrown.message.orEmpty())
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

    private fun writeTimer(result: String) = registry.find("balance.store.write.duration").tag("result", result).timer()

    @Test
    fun `write duration is timed as applied when the condition holds`() {
        succeed()

        writer.applyIfNewer(snapshot)

        assertEquals(1L, writeTimer("applied")?.count())
        assertEquals(0L, writeTimer("condition_failed")?.count())
        assertEquals(0L, writeTimer("error")?.count())
    }

    @Test
    fun `write duration is timed as condition failed for duplicates and obsolete events, which are not errors`() {
        failConditionWith(itemOf(transactionEvent()))
        writer.applyIfNewer(snapshot)
        failConditionWith(itemOf(transactionEvent(timestampMicros = 1751749453433999L)))
        writer.applyIfNewer(snapshot)

        assertEquals(2L, writeTimer("condition_failed")?.count())
        assertEquals(0L, writeTimer("error")?.count())
    }

    @Test
    fun `write duration is recorded as error when the sdk throws and is never tagged with account data`() {
        failWith(ApiCallTimeoutException.builder().message("x").build())

        assertFailsWith<BalanceStoreUnavailableException> { writer.applyIfNewer(snapshot) }

        assertEquals(1L, writeTimer("error")?.count())
        val tagKeys = registry.find("balance.store.write.duration").timers().flatMap { timer -> timer.id.tags.map { it.key } }.toSet()
        assertEquals(setOf("result"), tagKeys)
    }

    @Test
    fun `write duration publishes a histogram with the documented service level objectives`() {
        succeed()
        writer.applyIfNewer(snapshot)

        val buckets = writeTimer("applied")!!.takeSnapshot().histogramCounts().map { it.bucket(java.util.concurrent.TimeUnit.MILLISECONDS) }
        assertTrue(buckets.isNotEmpty(), "sem histograma")
        assertTrue(5.0 in buckets && 2000.0 in buckets, "buckets $buckets")
    }
}
