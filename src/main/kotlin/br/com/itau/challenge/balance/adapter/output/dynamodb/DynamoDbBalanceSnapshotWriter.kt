package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.math.BigDecimal

/**
 * Escrita do snapshot por UMA `UpdateItem` condicional (AP2, data-model.md 4.4): o proprio banco arbitra, de forma atomica
 * entre threads e instancias, se o evento supera o vigente pela precedencia `(lastTxTsMicros, lastTxId)`. Nunca le antes
 * (nada de read-modify-write) nem usa lock local, `BatchWriteItem` ou `TransactWriteItems`.
 *
 * - Condicao verdadeira (conta ausente ou precedencia maior) -> [ApplyResult.Applied]; todos os campos mudam juntos.
 * - `ConditionalCheckFailedException` nao e erro: o vigente tem precedencia maior ou igual. O item vigente vem na propria
 *   excecao (`ALL_OLD`, sem leitura extra) e classifica o desfecho: mesma `(lastTxTsMicros, lastTxId)` -> [ApplyResult.Duplicate]
 *   (`conflicting` se dono, situacao, moeda ou saldo divergem; saldo por `compareTo`, pois o DynamoDB pode normalizar
 *   `183.10` -> `183.1`); demais casos -> [ApplyResult.Obsolete]. Se a excecao nao trouxer o item (comportamento inesperado
 *   do endpoint), uma unica `GetItem` fortemente consistente o obtem (caminho raro).
 * - Demais falhas do SDK sao traduzidas por [DynamoDbExceptionTranslator.forWrite] e nunca engolidas; excecoes que nao
 *   sao do SDK propagam como estao.
 *
 * O `lastTxId` e comparado como string pelo banco (bytes UTF-8): o [BalanceItemMapper] grava o UUID canonico em minusculas.
 */
class DynamoDbBalanceSnapshotWriter(
    private val client: DynamoDbClient,
    private val tableName: String,
) : BalanceSnapshotWriter {
    override fun applyIfNewer(snapshot: BalanceSnapshot): ApplyResult {
        val item = BalanceItemMapper.toItem(snapshot)
        val request =
            UpdateItemRequest
                .builder()
                .tableName(tableName)
                .key(BalanceItemMapper.keyOf(snapshot.accountId))
                .updateExpression(UPDATE_EXPRESSION)
                .conditionExpression(CONDITION_EXPRESSION)
                .expressionAttributeValues(
                    mapOf(
                        ":v" to item.getValue(BalanceAttributes.SCHEMA_VERSION),
                        ":o" to item.getValue(BalanceAttributes.OWNER_ID),
                        ":st" to item.getValue(BalanceAttributes.ACCOUNT_STATUS),
                        ":amt" to item.getValue(BalanceAttributes.BALANCE_AMOUNT),
                        ":cur" to item.getValue(BalanceAttributes.BALANCE_CURRENCY),
                        ":cr" to item.getValue(BalanceAttributes.ACCOUNT_CREATED_AT_MICROS),
                        ":ts" to item.getValue(BalanceAttributes.LAST_TX_TS_MICROS),
                        ":tx" to item.getValue(BalanceAttributes.LAST_TX_ID),
                    ),
                ).returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                .build()
        return try {
            client.updateItem(request)
            ApplyResult.Applied
        } catch (failure: ConditionalCheckFailedException) {
            classify(snapshot, failure)
        } catch (failure: RuntimeException) {
            throw DynamoDbExceptionTranslator.forWrite(failure) ?: failure
        }
    }

    private fun classify(
        candidate: BalanceSnapshot,
        failure: ConditionalCheckFailedException,
    ): ApplyResult {
        val current = if (failure.hasItem()) failure.item() else fetchCurrent(candidate)
        val currentTimestamp = current.long(BalanceAttributes.LAST_TX_TS_MICROS)
        val currentTransactionId = current.text(BalanceAttributes.LAST_TX_ID)
        val byTimestamp = currentTimestamp.compareTo(candidate.precedence.timestamp.micros)
        val byTransactionId = currentTransactionId.compareTo(candidate.precedence.transactionId.value)
        val comparison = if (byTimestamp != 0) byTimestamp else byTransactionId
        return when {
            comparison == 0 -> ApplyResult.Duplicate(conflicting = diverges(candidate, current))
            comparison > 0 -> ApplyResult.Obsolete
            // A condicao falhou mas o vigente e inferior: leitura inconsistente do endpoint. Reentrega, nunca descarte.
            else -> throw BalanceStoreUnavailableException(StoreFailureCause.UNAVAILABLE)
        }
    }

    /** Caminho raro: a excecao veio sem o item; uma unica leitura fortemente consistente o obtem. Item ausente e transitorio. */
    private fun fetchCurrent(candidate: BalanceSnapshot): Map<String, AttributeValue> {
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
                throw DynamoDbExceptionTranslator.forWrite(failure) ?: failure
            }
        if (!response.hasItem()) throw BalanceStoreUnavailableException(StoreFailureCause.UNAVAILABLE)
        return response.item()
    }

    /** Conteudo divergente de um mesmo evento (data-model 4.4): dono, situacao, moeda ou saldo (`compareTo`, nao `equals`). */
    private fun diverges(
        candidate: BalanceSnapshot,
        current: Map<String, AttributeValue>,
    ): Boolean =
        current.text(BalanceAttributes.OWNER_ID) != candidate.ownerId.value ||
            current.text(BalanceAttributes.ACCOUNT_STATUS) != candidate.status.name ||
            current.text(BalanceAttributes.BALANCE_CURRENCY) != candidate.balance.currency.value ||
            current.decimal(BalanceAttributes.BALANCE_AMOUNT).compareTo(candidate.balance.amount) != 0

    /** Atributos ausentes ou ilegiveis do item vigente: falha interna sem valores na mensagem (nunca classifica no escuro). */
    private fun Map<String, AttributeValue>.text(name: String): String = this[name]?.s() ?: unreadable(name)

    private fun Map<String, AttributeValue>.long(name: String): Long =
        try {
            (this[name]?.n() ?: unreadable(name)).toLong()
        } catch (_: NumberFormatException) {
            unreadable(name)
        }

    private fun Map<String, AttributeValue>.decimal(name: String): BigDecimal =
        try {
            BigDecimal(this[name]?.n() ?: unreadable(name))
        } catch (_: NumberFormatException) {
            unreadable(name)
        }

    private fun unreadable(attribute: String): Nothing = throw IllegalStateException("unreadable current balance item: invalid attribute '$attribute'")

    private companion object {
        const val UPDATE_EXPRESSION =
            "SET schemaVersion = :v, ownerId = :o, accountStatus = :st, balanceAmount = :amt, balanceCurrency = :cur, " +
                "accountCreatedAtMicros = :cr, lastTxTsMicros = :ts, lastTxId = :tx"
        const val CONDITION_EXPRESSION =
            "attribute_not_exists(pk) OR lastTxTsMicros < :ts OR (lastTxTsMicros = :ts AND lastTxId < :tx)"
    }
}
