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

/** Nomes dos atributos do item `AccountBalances` (data-model.md 4.1). */
internal object BalanceAttributes {
    const val PK = "pk"
    const val SK = "sk"
    const val SCHEMA_VERSION = "schemaVersion"
    const val OWNER_ID = "ownerId"
    const val ACCOUNT_STATUS = "accountStatus"
    const val BALANCE_AMOUNT = "balanceAmount"
    const val BALANCE_CURRENCY = "balanceCurrency"
    const val ACCOUNT_CREATED_AT_MICROS = "accountCreatedAtMicros"
    const val LAST_TX_TS_MICROS = "lastTxTsMicros"
    const val LAST_TX_ID = "lastTxId"

    const val PK_PREFIX = "ACCOUNT#"
    const val SK_VALUE = "BALANCE"
    const val CURRENT_SCHEMA_VERSION = "1"
}

/**
 * Traduz entre [BalanceSnapshot] e o item do DynamoDB. `balanceAmount` e `N` escrito com `toPlainString()` (sem
 * notacao cientifica) e lido com `BigDecimal(String)`: o banco normaliza zeros a direita (`183.10` -> `183.1`) sem
 * alterar o valor.
 *
 * Um item que nao respeita o layout ou os limites do dominio (corrompido ou legado) e SEMPRE uma
 * [IllegalStateException] (falha interna), nunca `InvalidEventException` (que a API leria como requisicao invalida)
 * nem dado errado. A mensagem cita apenas o nome do atributo; jamais o valor.
 */
internal object BalanceItemMapper {
    fun keyOf(accountId: AccountId): Map<String, AttributeValue> =
        mapOf(
            BalanceAttributes.PK to text("${BalanceAttributes.PK_PREFIX}${accountId.value}"),
            BalanceAttributes.SK to text(BalanceAttributes.SK_VALUE),
        )

    fun toItem(snapshot: BalanceSnapshot): Map<String, AttributeValue> =
        keyOf(snapshot.accountId) +
            mapOf(
                BalanceAttributes.SCHEMA_VERSION to number(BalanceAttributes.CURRENT_SCHEMA_VERSION),
                BalanceAttributes.OWNER_ID to text(snapshot.ownerId.value),
                BalanceAttributes.ACCOUNT_STATUS to text(snapshot.status.name),
                BalanceAttributes.BALANCE_AMOUNT to number(snapshot.balance.amount.toPlainString()),
                BalanceAttributes.BALANCE_CURRENCY to text(snapshot.balance.currency.value),
                BalanceAttributes.ACCOUNT_CREATED_AT_MICROS to number(snapshot.accountCreatedAt.micros.toString()),
                BalanceAttributes.LAST_TX_TS_MICROS to number(snapshot.precedence.timestamp.micros.toString()),
                BalanceAttributes.LAST_TX_ID to text(snapshot.precedence.transactionId.value),
            )

    fun fromItem(item: Map<String, AttributeValue>): BalanceSnapshot {
        if (item.text(BalanceAttributes.SK) != BalanceAttributes.SK_VALUE) corrupted(BalanceAttributes.SK)
        if (item.number(BalanceAttributes.SCHEMA_VERSION) != BalanceAttributes.CURRENT_SCHEMA_VERSION) {
            corrupted(BalanceAttributes.SCHEMA_VERSION)
        }
        val partitionKey = item.text(BalanceAttributes.PK)
        if (!partitionKey.startsWith(BalanceAttributes.PK_PREFIX)) corrupted(BalanceAttributes.PK)

        return BalanceSnapshot(
            accountId = parsed(BalanceAttributes.PK) { AccountId.parse(partitionKey.removePrefix(BalanceAttributes.PK_PREFIX)) },
            ownerId = parsed(BalanceAttributes.OWNER_ID) { OwnerId.parse(item.text(BalanceAttributes.OWNER_ID)) },
            status = parsed(BalanceAttributes.ACCOUNT_STATUS) { AccountStatus.parse(item.text(BalanceAttributes.ACCOUNT_STATUS)) },
            balance =
                parsed(BalanceAttributes.BALANCE_AMOUNT) {
                    Money.of(
                        BigDecimal(item.number(BalanceAttributes.BALANCE_AMOUNT)),
                        CurrencyCode.parse(item.text(BalanceAttributes.BALANCE_CURRENCY)),
                    )
                },
            accountCreatedAt =
                parsed(BalanceAttributes.ACCOUNT_CREATED_AT_MICROS) {
                    EventInstant.accountCreatedAt(item.number(BalanceAttributes.ACCOUNT_CREATED_AT_MICROS).toLong())
                },
            precedence =
                Precedence(
                    timestamp =
                        parsed(BalanceAttributes.LAST_TX_TS_MICROS) {
                            EventInstant.transactionTimestamp(item.number(BalanceAttributes.LAST_TX_TS_MICROS).toLong())
                        },
                    transactionId = parsed(BalanceAttributes.LAST_TX_ID) { TransactionId.parse(item.text(BalanceAttributes.LAST_TX_ID)) },
                ),
        )
    }

    private fun text(value: String): AttributeValue = AttributeValue.builder().s(value).build()

    private fun number(value: String): AttributeValue = AttributeValue.builder().n(value).build()

    private fun Map<String, AttributeValue>.text(name: String): String = this[name]?.s() ?: corrupted(name)

    private fun Map<String, AttributeValue>.number(name: String): String = this[name]?.n() ?: corrupted(name)

    /**
     * Executa a conversao de um atributo e a normaliza: falhas de validacao do dominio ou de parse numerico (`NumberFormatException`, inclusive expoente fora de faixa) viram
     * [IllegalStateException] sem causa (a causa de um parser pode conter o valor).
     */
    private fun <T> parsed(
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
