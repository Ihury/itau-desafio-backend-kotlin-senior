package br.com.itau.challenge.balance.support

import br.com.itau.challenge.balance.testing.toEpochMicros
import java.math.BigDecimal
import java.time.Instant

class Defect(
    val description: String,
    val payload: ByteArray,
    val expectedReason: String,
    val expectedDetailPath: String?,
    val account: String? = null,
)

private const val ONE_HOUR_SECONDS = 3600L

fun validPayload(
    account: String,
    timestampMicros: Long = EventPayloads.BASE_TIMESTAMP_MICROS,
    amount: String = "5.00",
    createdAtMicros: Long = EventPayloads.DEFAULT_ACCOUNT_CREATED_AT_MICROS,
): String = EventPayloads.transaction(account, timestampMicros = timestampMicros, balanceAmount = amount, accountCreatedAtMicros = createdAtMicros)

private fun defect(
    description: String,
    json: String,
    expectedReason: String,
    expectedDetailPath: String?,
    account: String?,
) = Defect(description, json.toByteArray(Charsets.UTF_8), expectedReason, expectedDetailPath, account)

fun oneDefectOfEachKind(newAccount: () -> String): List<Defect> {
    val beyondFutureTolerance = Instant.now().plusSeconds(ONE_HOUR_SECONDS).toEpochMicros()
    val missingOwnerAccount = newAccount()
    val badTransactionIdAccount = newAccount()
    val badCurrencyAccount = newAccount()
    val stringAmountAccount = newAccount()
    val millisecondsAccount = newAccount()
    val futureAccount = newAccount()
    val badTypeAccount = newAccount()
    val badStatusAccount = newAccount()
    return listOf(
        defect("malformed", "{not json", "malformed_payload", null, null),
        defect("missing owner", validPayload(missingOwnerAccount).replace(""""owner":"${EventPayloads.DEFAULT_OWNER}",""", ""), "missing_field", "account.owner", missingOwnerAccount),
        defect("bad transaction id", EventPayloads.transaction(badTransactionIdAccount, transactionId = "1-1-1-1-1"), "invalid_identifier", "transaction.id", badTransactionIdAccount),
        defect("bad currency", EventPayloads.transaction(badCurrencyAccount, currency = "brl"), "invalid_currency", "transaction.currency", badCurrencyAccount),
        defect("string amount", EventPayloads.transaction(stringAmountAccount, balanceAmount = "\"10.00\""), "invalid_value", "account.balance.amount", stringAmountAccount),
        defect("milliseconds", validPayload(millisecondsAccount, timestampMicros = 1751749453433L), "invalid_timestamp", "transaction.timestamp", millisecondsAccount),
        defect("future beyond tolerance", validPayload(futureAccount, timestampMicros = beyondFutureTolerance), "invalid_timestamp", "transaction.timestamp", futureAccount),
        defect("bad type", EventPayloads.transaction(badTypeAccount, transactionType = "TRANSFER"), "unknown_domain_value", "transaction.type", badTypeAccount),
        defect("bad status", EventPayloads.transaction(badStatusAccount, accountStatus = "SUSPENDED"), "unknown_domain_value", "account.status", badStatusAccount),
    )
}

fun publishValidBatch(
    topics: TopicSet,
    newAccount: () -> String,
    count: Int,
    interleavedDefects: List<Defect> = emptyList(),
) {
    val defectsEvery = if (interleavedDefects.isEmpty()) Int.MAX_VALUE else count / interleavedDefects.size
    var nextDefect = 0
    repeat(count) { index ->
        topics.publish(validPayload(newAccount(), timestampMicros = EventPayloads.BASE_TIMESTAMP_MICROS + index, amount = BigDecimal(index).toPlainString()))
        if ((index + 1) % defectsEvery == 0 && nextDefect < interleavedDefects.size) topics.publish(interleavedDefects[nextDefect++].payload)
    }
    while (nextDefect < interleavedDefects.size) topics.publish(interleavedDefects[nextDefect++].payload)
}
