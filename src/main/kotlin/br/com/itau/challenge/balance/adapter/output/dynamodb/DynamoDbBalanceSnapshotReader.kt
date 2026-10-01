package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotReader
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest

class DynamoDbBalanceSnapshotReader(
    private val client: DynamoDbClient,
    private val tableName: String,
    private val consistentRead: Boolean,
    meterRegistry: MeterRegistry,
) : BalanceSnapshotReader {
    private val corruptedItems =
        Counter
            .builder("balance.store.read.corrupted")
            .description("Itens do snapshot ilegiveis ou fora dos limites do dominio (falha interna; alertar)")
            .register(meterRegistry)
    private val timers = readTimers(meterRegistry)

    override fun find(accountId: AccountId): BalanceSnapshot? {
        val request =
            GetItemRequest
                .builder()
                .tableName(tableName)
                .key(BalanceItemMapper.keyOf(accountId))
                .consistentRead(consistentRead)
                .build()
        val startedNanos = timers.startNanos()
        val response =
            try {
                client.getItem(request)
            } catch (failure: RuntimeException) {
                timers.record(ReadResult.ERROR, startedNanos)
                throw DynamoDbExceptionTranslator.translateReadFailure(failure)
            }
        timers.record(if (response.hasItem()) ReadResult.FOUND else ReadResult.NOT_FOUND, startedNanos)
        if (!response.hasItem()) return null
        return try {
            BalanceItemMapper.fromItem(response.item())
        } catch (failure: IllegalStateException) {
            corruptedItems.increment()
            log.error("balance item cannot be mapped accountId={} reason={}", accountId, failure.message)
            throw failure
        }
    }

    private companion object {
        private val log = LoggerFactory.getLogger(DynamoDbBalanceSnapshotReader::class.java)
    }
}
