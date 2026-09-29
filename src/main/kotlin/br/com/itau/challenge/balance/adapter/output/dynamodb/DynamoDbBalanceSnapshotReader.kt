package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotReader
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest

/**
 * Leitura do snapshot por `GetItem` na chave primaria (AP1), fortemente consistente por padrao (FR-027).
 *
 * - Item ausente -> `null`; item presente -> snapshot mapeado.
 * - QUALQUER falha do SDK -> `BalanceStoreUnavailableException` (jamais `null`: nunca um falso "nao encontrada").
 * - Item corrompido/legado (fora do layout ou dos limites do dominio) -> `IllegalStateException` (falha interna), com
 *   log e metrica `balance.store.read.corrupted`; nunca vira `InvalidEventException`, 404 ou dado errado.
 */
class DynamoDbBalanceSnapshotReader(
    private val client: DynamoDbClient,
    private val tableName: String,
    private val consistentRead: Boolean,
    meterRegistry: MeterRegistry,
) : BalanceSnapshotReader {
    private val corrupted = meterRegistry.counter("balance.store.read.corrupted")

    override fun find(accountId: AccountId): BalanceSnapshot? {
        val request =
            GetItemRequest
                .builder()
                .tableName(tableName)
                .key(BalanceItemMapper.keyOf(accountId))
                .consistentRead(consistentRead)
                .build()
        val response =
            try {
                client.getItem(request)
            } catch (failure: RuntimeException) {
                throw DynamoDbExceptionTranslator.forRead(failure) ?: failure
            }
        if (!response.hasItem()) return null
        return try {
            BalanceItemMapper.fromItem(response.item())
        } catch (failure: IllegalStateException) {
            corrupted.increment()
            log.error("balance item cannot be mapped accountId={} reason={}", accountId, failure.message)
            throw failure
        }
    }

    private companion object {
        private val log = LoggerFactory.getLogger(DynamoDbBalanceSnapshotReader::class.java)
    }
}
