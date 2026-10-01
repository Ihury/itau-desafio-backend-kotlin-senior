package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.AccountStatus
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.CurrencyCode
import br.com.itau.challenge.balance.domain.model.EventInstant
import br.com.itau.challenge.balance.domain.model.Money
import br.com.itau.challenge.balance.domain.model.OwnerId
import br.com.itau.challenge.balance.domain.model.Precedence
import br.com.itau.challenge.balance.domain.model.TransactionId
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.math.BigDecimal

internal object BalanceItemMapper {
    fun keyOf(accountId: AccountId): Map<String, AttributeValue> =
        mapOf(
            BalanceAttributes.PK to stringAttribute("${BalanceAttributes.PK_PREFIX}${accountId.value}"),
            BalanceAttributes.SK to stringAttribute(BalanceAttributes.SK_VALUE),
        )

    fun toItem(snapshot: BalanceSnapshot): Map<String, AttributeValue> =
        keyOf(snapshot.accountId) +
            mapOf(
                BalanceAttributes.SCHEMA_VERSION to numberAttribute(BalanceAttributes.CURRENT_SCHEMA_VERSION),
                BalanceAttributes.OWNER_ID to stringAttribute(snapshot.ownerId.value),
                BalanceAttributes.ACCOUNT_STATUS to stringAttribute(snapshot.status.name),
                BalanceAttributes.BALANCE_AMOUNT to numberAttribute(snapshot.balance.amount.toPlainString()),
                BalanceAttributes.BALANCE_CURRENCY to stringAttribute(snapshot.balance.currency.value),
                BalanceAttributes.ACCOUNT_CREATED_AT_MICROS to numberAttribute(snapshot.accountCreatedAt.micros.toString()),
                BalanceAttributes.LAST_TX_TS_MICROS to numberAttribute(snapshot.precedence.timestamp.micros.toString()),
                BalanceAttributes.LAST_TX_ID to stringAttribute(snapshot.precedence.transactionId.value),
            )

    fun fromItem(item: Map<String, AttributeValue>): BalanceSnapshot {
        if (item.text(BalanceAttributes.SK) != BalanceAttributes.SK_VALUE) corrupted(BalanceAttributes.SK)
        if (item.number(BalanceAttributes.SCHEMA_VERSION) != BalanceAttributes.CURRENT_SCHEMA_VERSION) {
            corrupted(BalanceAttributes.SCHEMA_VERSION)
        }
        return BalanceSnapshot(
            accountId = accountIdOf(item),
            ownerId = ownerOf(item),
            status = statusOf(item),
            balance = balanceOf(item),
            accountCreatedAt = instantOf(item, BalanceAttributes.ACCOUNT_CREATED_AT_MICROS),
            precedence = precedenceOf(item),
        )
    }

    fun ownerOf(item: Map<String, AttributeValue>): OwnerId =
        readOrCorrupted(BalanceAttributes.OWNER_ID) { OwnerId.parse(item.text(BalanceAttributes.OWNER_ID)) }

    fun statusOf(item: Map<String, AttributeValue>): AccountStatus =
        readOrCorrupted(BalanceAttributes.ACCOUNT_STATUS) { AccountStatus.parse(item.text(BalanceAttributes.ACCOUNT_STATUS)) }

    fun balanceOf(item: Map<String, AttributeValue>): Money =
        readOrCorrupted(BalanceAttributes.BALANCE_AMOUNT) {
            Money.of(
                BigDecimal(item.number(BalanceAttributes.BALANCE_AMOUNT)),
                CurrencyCode.parse(item.text(BalanceAttributes.BALANCE_CURRENCY)),
            )
        }

    fun precedenceOf(item: Map<String, AttributeValue>): Precedence =
        Precedence(
            timestamp = instantOf(item, BalanceAttributes.LAST_TX_TS_MICROS),
            transactionId = readOrCorrupted(BalanceAttributes.LAST_TX_ID) { TransactionId.parse(item.text(BalanceAttributes.LAST_TX_ID)) },
        )

    private fun accountIdOf(item: Map<String, AttributeValue>): AccountId {
        val partitionKey = item.text(BalanceAttributes.PK)
        if (!partitionKey.startsWith(BalanceAttributes.PK_PREFIX)) corrupted(BalanceAttributes.PK)
        return readOrCorrupted(BalanceAttributes.PK) { AccountId.parse(partitionKey.removePrefix(BalanceAttributes.PK_PREFIX)) }
    }

    private fun instantOf(
        item: Map<String, AttributeValue>,
        attribute: String,
    ): EventInstant = readOrCorrupted(attribute) { EventInstant.fromPersisted(item.number(attribute).toLong()) }

    private fun stringAttribute(value: String): AttributeValue = AttributeValue.builder().s(value).build()

    private fun numberAttribute(value: String): AttributeValue = AttributeValue.builder().n(value).build()

    private fun Map<String, AttributeValue>.text(name: String): String = this[name]?.s() ?: corrupted(name)

    private fun Map<String, AttributeValue>.number(name: String): String = this[name]?.n() ?: corrupted(name)

    private fun <T> readOrCorrupted(
        attribute: String,
        block: () -> T,
    ): T =
        try {
            block()
        } catch (_: InvalidEventException) {
            corrupted(attribute)
        } catch (_: NumberFormatException) {
            corrupted(attribute)
        }

    private fun corrupted(attribute: String): Nothing = throw IllegalStateException("corrupted balance item: invalid attribute '$attribute'")
}
