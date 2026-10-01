package br.com.itau.challenge.balance.adapter.output.dynamodb

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
