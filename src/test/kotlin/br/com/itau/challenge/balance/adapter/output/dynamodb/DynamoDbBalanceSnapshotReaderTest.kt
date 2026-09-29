package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.domain.model.TransactionEventFixtures.DEFAULT_ACCOUNT_ID
import br.com.itau.challenge.balance.domain.model.TransactionEventFixtures.transactionEvent
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import software.amazon.awssdk.core.exception.ApiCallTimeoutException
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DynamoDbBalanceSnapshotReaderTest {
    private val client = mock(DynamoDbClient::class.java)
    private val registry = SimpleMeterRegistry()
    private val accountId = AccountId.parse(DEFAULT_ACCOUNT_ID)
    private val snapshot = BalanceSnapshot.from(transactionEvent())

    private fun reader(consistent: Boolean = true) = DynamoDbBalanceSnapshotReader(client, "AccountBalances", consistent, registry)

    private fun respondWith(item: Map<String, AttributeValue>?) {
        val response = if (item == null) GetItemResponse.builder().build() else GetItemResponse.builder().item(item).build()
        doReturn(response).`when`(client).getItem(any(GetItemRequest::class.java))
    }

    private fun capturedRequest(): GetItemRequest {
        val captor = ArgumentCaptor.forClass(GetItemRequest::class.java)
        verify(client).getItem(captor.capture())
        return captor.value
    }

    @Test
    fun `reads by primary key with consistent read by default`() {
        respondWith(BalanceItemMapper.toItem(snapshot))

        reader().find(accountId)

        val request = capturedRequest()
        assertEquals("AccountBalances", request.tableName())
        assertEquals(BalanceItemMapper.keyOf(accountId), request.key())
        assertEquals(true, request.consistentRead())
    }

    @Test
    fun `consistent read can be turned off by configuration`() {
        respondWith(BalanceItemMapper.toItem(snapshot))

        reader(consistent = false).find(accountId)

        assertEquals(false, capturedRequest().consistentRead())
    }

    @Test
    fun `absent item is null`() {
        respondWith(null)

        assertNull(reader().find(accountId))
    }

    @Test
    fun `present item is mapped to the snapshot`() {
        respondWith(BalanceItemMapper.toItem(snapshot))

        assertEquals(snapshot, reader().find(accountId))
    }

    @Test
    fun `any sdk failure becomes store unavailable and never null`() {
        val cases =
            mapOf(
                ProvisionedThroughputExceededException.builder().message("x").build() to StoreFailureCause.THROTTLED,
                ApiCallTimeoutException.builder().message("x").build() to StoreFailureCause.TIMEOUT,
                SdkClientException.builder().message("x").build() to StoreFailureCause.UNAVAILABLE,
            )
        cases.forEach { (failure, expected) ->
            doThrow(failure).`when`(client).getItem(any(GetItemRequest::class.java))

            val raised = assertFailsWith<BalanceStoreUnavailableException> { reader().find(accountId) }

            assertEquals(expected, raised.failureCause)
        }
    }

    @Test
    fun `corrupted item propagates IllegalStateException and never becomes invalid event, null or unavailable`() {
        val corrupted = BalanceItemMapper.toItem(snapshot).toMutableMap()
        corrupted["accountStatus"] = AttributeValue.builder().s("SUSPENDED").build()
        respondWith(corrupted)

        val failure = assertFailsWith<IllegalStateException> { reader().find(accountId) }

        assertFalse(InvalidEventException::class.java.isInstance(failure))
        assertFalse("183" in failure.message.orEmpty(), "sem dados de saldo na mensagem")
    }

    @Test
    fun `corrupted item is counted in a metric so it can be alerted on`() {
        val corrupted = BalanceItemMapper.toItem(snapshot).toMutableMap()
        corrupted.remove("balanceAmount")
        respondWith(corrupted)

        assertFailsWith<IllegalStateException> { reader().find(accountId) }

        assertEquals(1.0, registry.get("balance.store.read.corrupted").counter().count())
        assertTrue(registry.find("balance.store.read.corrupted").counter()?.id?.tags.orEmpty().isEmpty())
    }
}
