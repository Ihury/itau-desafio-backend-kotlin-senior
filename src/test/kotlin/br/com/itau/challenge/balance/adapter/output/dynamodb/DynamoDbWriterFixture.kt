package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.TransactionEvent
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.transactionEvent
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse

abstract class DynamoDbWriterFixture {
    protected val client: DynamoDbClient = mock(DynamoDbClient::class.java)
    protected val registry = SimpleMeterRegistry()
    protected val writer = DynamoDbBalanceSnapshotWriter(client, "AccountBalances", registry)
    protected val snapshot = BalanceSnapshot.from(transactionEvent())

    protected fun stubUpdateSucceeds() {
        doReturn(UpdateItemResponse.builder().build()).`when`(client).updateItem(any(UpdateItemRequest::class.java))
    }

    protected fun stubUpdateFailsWith(failure: Throwable) {
        doThrow(failure).`when`(client).updateItem(any(UpdateItemRequest::class.java))
    }

    protected fun itemOf(event: TransactionEvent) = BalanceItemMapper.toItem(BalanceSnapshot.from(event))

    protected fun stubConditionFailedWithCurrentItem(current: Map<String, AttributeValue>) {
        stubUpdateFailsWith(ConditionalCheckFailedException.builder().message("The conditional request failed").item(current).build())
    }

    protected fun stubConditionFailedWithoutCurrentItem() {
        stubUpdateFailsWith(ConditionalCheckFailedException.builder().message("The conditional request failed").build())
    }

    protected fun stubGetItem(item: Map<String, AttributeValue>?) {
        val response = if (item == null) GetItemResponse.builder().build() else GetItemResponse.builder().item(item).build()
        doReturn(response).`when`(client).getItem(any(GetItemRequest::class.java))
    }

    protected fun capturedUpdateRequest(): UpdateItemRequest {
        val captor = ArgumentCaptor.forClass(UpdateItemRequest::class.java)
        verify(client, times(1)).updateItem(captor.capture())
        return captor.value
    }
}
