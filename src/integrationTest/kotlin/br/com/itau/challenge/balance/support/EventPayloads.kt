package br.com.itau.challenge.balance.support

import br.com.itau.challenge.balance.testing.TransactionEventFixtures
import java.util.UUID

object EventPayloads {
    const val DEFAULT_OWNER = TransactionEventFixtures.DEFAULT_OWNER_ID
    const val BASE_TIMESTAMP_MICROS = TransactionEventFixtures.DEFAULT_TIMESTAMP_MICROS
    const val DEFAULT_ACCOUNT_CREATED_AT_MICROS = TransactionEventFixtures.DEFAULT_ACCOUNT_CREATED_AT_MICROS
    const val DEFAULT_TRANSACTION_ID = TransactionEventFixtures.DEFAULT_TRANSACTION_ID

    @Suppress("LongParameterList")
    fun transaction(
        accountId: String,
        timestampMicros: Long = BASE_TIMESTAMP_MICROS,
        transactionId: String = UUID.randomUUID().toString(),
        balanceAmount: String = "183.12",
        currency: String = "BRL",
        ownerId: String = DEFAULT_OWNER,
        accountStatus: String = "ENABLED",
        transactionStatus: String = "APPROVED",
        transactionType: String = "CREDIT",
        transactionAmount: String = "97.07",
        accountCreatedAtMicros: Long = DEFAULT_ACCOUNT_CREATED_AT_MICROS,
    ): String =
        """{"transaction":{"id":"$transactionId","type":"$transactionType","amount":$transactionAmount,"currency":"$currency",""" +
            """"status":"$transactionStatus","timestamp":$timestampMicros},"account":{"id":"$accountId","owner":"$ownerId",""" +
            """"created_at":$accountCreatedAtMicros,"status":"$accountStatus","balance":{"amount":$balanceAmount,"currency":"$currency"}}}"""
}
