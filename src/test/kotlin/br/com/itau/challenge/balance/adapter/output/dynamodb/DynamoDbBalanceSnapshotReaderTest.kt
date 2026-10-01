package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_ACCOUNT_ID
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.transactionEvent
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
import java.util.concurrent.TimeUnit
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
        assertTrue(request.consistentRead())
    }

    @Test
    fun `consistent read can be turned off by configuration`() {
        respondWith(BalanceItemMapper.toItem(snapshot))

        reader(consistent = false).find(accountId)

        assertFalse(capturedRequest().consistentRead())
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

    private fun readTimer(result: String) = registry.find("balance.store.read.duration").tag("result", result).timer()

    @Test
    fun `read duration is timed per result and never tagged with account data`() {
        respondWith(BalanceItemMapper.toItem(snapshot))
        reader().find(accountId)
        respondWith(null)
        reader().find(accountId)
        respondWith(null)
        reader().find(accountId)

        assertEquals(1L, readTimer("found")?.count())
        assertEquals(2L, readTimer("not_found")?.count())
        assertEquals(0L, readTimer("error")?.count(), "a serie de erro existe em zero antes da primeira falha")
        val tagKeys = registry.find("balance.store.read.duration").timers().flatMap { timer -> timer.id.tags.map { it.key } }.toSet()
        assertEquals(setOf("result"), tagKeys)
    }

    @Test
    fun `read duration is recorded even when the sdk throws`() {
        doThrow(SdkClientException.builder().message("x").build()).`when`(client).getItem(any(GetItemRequest::class.java))

        assertFailsWith<BalanceStoreUnavailableException> { reader().find(accountId) }

        assertEquals(1L, readTimer("error")?.count())
        assertEquals(0L, readTimer("found")?.count())
    }

    @Test
    fun `a corrupted item still counts as found because the database answered`() {
        val corrupted = BalanceItemMapper.toItem(snapshot).toMutableMap()
        corrupted.remove("balanceAmount")
        respondWith(corrupted)

        assertFailsWith<IllegalStateException> { reader().find(accountId) }

        assertEquals(1L, readTimer("found")?.count())
    }

    @Test
    fun `read duration publishes a histogram with the documented service level objectives`() {
        respondWith(null)
        reader().find(accountId)

        val buckets = readTimer("not_found")!!.takeSnapshot().histogramCounts().map { it.bucket(TimeUnit.MILLISECONDS) }
        assertTrue(buckets.isNotEmpty(), "sem histograma")
        assertTrue(5.0 in buckets && 2000.0 in buckets, "buckets $buckets")
    }

    @Test
    fun `the corrupted item counter is described like the other outcome counters`() {
        reader()

        val counter = registry.get("balance.store.read.corrupted").counter()
        assertTrue(!counter.id.description.isNullOrBlank())
        assertEquals(0.0, counter.count())
    }
}
