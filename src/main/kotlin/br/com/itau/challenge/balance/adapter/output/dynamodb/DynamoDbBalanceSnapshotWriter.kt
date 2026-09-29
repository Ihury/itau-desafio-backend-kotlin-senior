package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest

/**
 * Escrita do snapshot por UMA `UpdateItem` condicional (AP2, data-model.md 4.4): o proprio banco arbitra, de forma atomica
 * entre threads e instancias, se o evento supera o vigente pela precedencia `(lastTxTsMicros, lastTxId)`. Nunca le antes
 * (nada de read-modify-write) nem usa lock local, `BatchWriteItem` ou `TransactWriteItems`.
 *
 * - Condicao verdadeira (conta ausente ou precedencia maior) -> [ApplyResult.Applied]; todos os campos mudam juntos.
 * - `ConditionalCheckFailedException` nao e erro: o vigente tem precedencia maior ou igual -> [ApplyResult.Obsolete]. A
 *   distincao entre duplicado e obsoleto (`ALL_OLD`) e refinada na convergencia (US3).
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
        } catch (_: ConditionalCheckFailedException) {
            ApplyResult.Obsolete
        } catch (failure: RuntimeException) {
            throw DynamoDbExceptionTranslator.forWrite(failure) ?: failure
        }
    }

    private companion object {
        const val UPDATE_EXPRESSION =
            "SET schemaVersion = :v, ownerId = :o, accountStatus = :st, balanceAmount = :amt, balanceCurrency = :cur, " +
                "accountCreatedAtMicros = :cr, lastTxTsMicros = :ts, lastTxId = :tx"
        const val CONDITION_EXPRESSION =
            "attribute_not_exists(pk) OR lastTxTsMicros < :ts OR (lastTxTsMicros = :ts AND lastTxId < :tx)"
    }
}
