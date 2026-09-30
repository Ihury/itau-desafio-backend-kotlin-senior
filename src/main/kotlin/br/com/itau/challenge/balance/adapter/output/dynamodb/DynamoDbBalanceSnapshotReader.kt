package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotReader
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import java.util.concurrent.TimeUnit

/**
 * Leitura do snapshot por `GetItem` na chave primaria (AP1), fortemente consistente por padrao (FR-027).
 *
 * - Item ausente -> `null`; item presente -> snapshot mapeado.
 * - QUALQUER falha do SDK -> `BalanceStoreUnavailableException` (jamais `null`: nunca um falso "nao encontrada").
 * - Item corrompido/legado (fora do layout ou dos limites do dominio) -> `IllegalStateException` (falha interna), com
 *   log e metrica `balance.store.read.corrupted`; nunca vira `InvalidEventException`, 404 ou dado errado.
 *
 * A latencia do `GetItem` vai para o timer `balance.store.read.duration{result=found|not_found|error}` (com histograma), inclusive
 * quando o SDK lanca; item corrompido conta como `found` (o banco respondeu). Nenhuma tag carrega dado da conta.
 */
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
    private val readTimers: Map<String, Timer> = RESULTS.associateWith { result -> readTimer(meterRegistry, result) }

    override fun find(accountId: AccountId): BalanceSnapshot? {
        val request =
            GetItemRequest
                .builder()
                .tableName(tableName)
                .key(BalanceItemMapper.keyOf(accountId))
                .consistentRead(consistentRead)
                .build()
        val startedNanos = System.nanoTime()
        val response =
            try {
                client.getItem(request)
            } catch (failure: RuntimeException) {
                recordDuration(ERROR, startedNanos)
                throw DynamoDbExceptionTranslator.translateReadFailure(failure) ?: failure
            }
        recordDuration(if (response.hasItem()) FOUND else NOT_FOUND, startedNanos)
        if (!response.hasItem()) return null
        return try {
            BalanceItemMapper.fromItem(response.item())
        } catch (failure: IllegalStateException) {
            corruptedItems.increment()
            log.error("balance item cannot be mapped accountId={} reason={}", accountId, failure.message)
            throw failure
        }
    }

    private fun recordDuration(
        result: String,
        startedNanos: Long,
    ) = readTimers.getValue(result).record(System.nanoTime() - startedNanos, TimeUnit.NANOSECONDS)

    private companion object {
        private val log = LoggerFactory.getLogger(DynamoDbBalanceSnapshotReader::class.java)
        const val FOUND = "found"
        const val NOT_FOUND = "not_found"
        const val ERROR = "error"
        val RESULTS = listOf(FOUND, NOT_FOUND, ERROR)

        /** Timer com histograma (SLO de 5 ms a 2 s); as tres series nascem em zero para as consultas enxergarem a serie. */
        fun readTimer(
            registry: MeterRegistry,
            result: String,
        ): Timer =
            Timer
                .builder("balance.store.read.duration")
                .description("Latencia do GetItem do snapshot")
                .tag("result", result)
                .serviceLevelObjectives(*StoreLatencyObjectives.OBJECTIVES)
                .register(registry)
    }
}
