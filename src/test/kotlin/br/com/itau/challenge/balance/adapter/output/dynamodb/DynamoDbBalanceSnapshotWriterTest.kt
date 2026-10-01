package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_OWNER_ID
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.transactionEvent
import br.com.itau.challenge.balance.testing.numberAttr
import br.com.itau.challenge.balance.testing.stringAttr
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure
import kotlin.test.assertEquals

class DynamoDbBalanceSnapshotWriterTest : DynamoDbWriterFixture() {
    @Test
    fun `the write is a single conditional UpdateItem with the exact update and condition expressions`() {
        stubUpdateSucceeds()

        writer.applyIfNewer(snapshot)

        val request = capturedUpdateRequest()
        assertEquals("AccountBalances", request.tableName())
        assertEquals(BalanceItemMapper.keyOf(snapshot.accountId), request.key())
        assertEquals(
            "SET schemaVersion = :v, ownerId = :o, accountStatus = :st, balanceAmount = :amt, balanceCurrency = :cur, " +
                "accountCreatedAtMicros = :cr, lastTxTsMicros = :ts, lastTxId = :tx",
            request.updateExpression(),
        )
        assertEquals(
            "attribute_not_exists(pk) OR lastTxTsMicros < :ts OR (lastTxTsMicros = :ts AND lastTxId < :tx)",
            request.conditionExpression(),
        )
        assertEquals(ReturnValuesOnConditionCheckFailure.ALL_OLD, request.returnValuesOnConditionCheckFailure())
    }

    @Test
    fun `values carry the snapshot with numbers as N and the transaction id in lower case`() {
        stubUpdateSucceeds()
        val upper = BalanceSnapshot.from(transactionEvent(transactionId = "8E8AE808-B154-48B5-9F3E-553935CC4543", timestampMicros = 1751749453433123L))

        writer.applyIfNewer(upper)

        val values = capturedUpdateRequest().expressionAttributeValues()
        assertEquals(
            setOf(":v", ":o", ":st", ":amt", ":cur", ":cr", ":ts", ":tx"),
            values.keys,
        )
        assertEquals(numberAttr("1"), values[":v"])
        assertEquals(stringAttr(DEFAULT_OWNER_ID), values[":o"])
        assertEquals(stringAttr("ENABLED"), values[":st"])
        assertEquals(numberAttr("183.12"), values[":amt"])
        assertEquals(stringAttr("BRL"), values[":cur"])
        assertEquals(numberAttr("1634874339000000"), values[":cr"])
        assertEquals(numberAttr("1751749453433123"), values[":ts"])
        assertEquals(stringAttr("8e8ae808-b154-48b5-9f3e-553935cc4543"), values[":tx"])
    }

    @Test
    fun `the balance is written as a plain decimal never in scientific notation`() {
        stubUpdateSucceeds()

        writer.applyIfNewer(BalanceSnapshot.from(transactionEvent(balanceAmount = "0.0000001")))
        val small = capturedUpdateRequest().expressionAttributeValues().getValue(":amt")

        assertEquals("0.0000001", small.n())
    }

    @Test
    fun `a successful update is applied`() {
        stubUpdateSucceeds()

        assertEquals(ApplyResult.Applied, writer.applyIfNewer(snapshot))
    }
}
