package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest

internal object BalanceUpdateRequestFactory {
    private const val UPDATE_EXPRESSION =
        "SET schemaVersion = :v, ownerId = :o, accountStatus = :st, balanceAmount = :amt, balanceCurrency = :cur, " +
            "accountCreatedAtMicros = :cr, lastTxTsMicros = :ts, lastTxId = :tx"
    private const val CONDITION_EXPRESSION =
        "attribute_not_exists(pk) OR lastTxTsMicros < :ts OR (lastTxTsMicros = :ts AND lastTxId < :tx)"

    fun create(
        tableName: String,
        snapshot: BalanceSnapshot,
    ): UpdateItemRequest {
        val item = BalanceItemMapper.toItem(snapshot)
        return UpdateItemRequest
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
    }
}
