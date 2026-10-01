package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import io.micrometer.core.instrument.MeterRegistry
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest

class DynamoDbBalanceSnapshotWriter(
    private val client: DynamoDbClient,
    private val tableName: String,
    meterRegistry: MeterRegistry,
) : BalanceSnapshotWriter {
    private val timers = writeTimers(meterRegistry)

    override fun applyIfNewer(snapshot: BalanceSnapshot): ApplyResult {
        val request = BalanceUpdateRequestFactory.create(tableName, snapshot)
        val startedNanos = timers.startNanos()
        try {
            client.updateItem(request)
        } catch (failure: ConditionalCheckFailedException) {
            timers.record(WriteResult.CONDITION_FAILED, startedNanos)
            val currentItem = if (failure.hasItem()) failure.item() else fetchCurrentItemConsistently(snapshot)
            return ConflictClassifier.classify(snapshot, currentItem)
        } catch (failure: RuntimeException) {
            timers.record(WriteResult.ERROR, startedNanos)
            throw DynamoDbExceptionTranslator.translateWriteFailure(failure)
        }
        timers.record(WriteResult.APPLIED, startedNanos)
        return ApplyResult.Applied
    }

    private fun fetchCurrentItemConsistently(candidate: BalanceSnapshot): Map<String, AttributeValue> {
        val request =
            GetItemRequest
                .builder()
                .tableName(tableName)
                .key(BalanceItemMapper.keyOf(candidate.accountId))
                .consistentRead(true)
                .build()
        val response =
            try {
                client.getItem(request)
            } catch (failure: RuntimeException) {
                throw DynamoDbExceptionTranslator.translateWriteFailure(failure)
            }
        if (!response.hasItem()) throw BalanceStoreUnavailableException(StoreFailureCause.UNAVAILABLE)
        return response.item()
    }
}
