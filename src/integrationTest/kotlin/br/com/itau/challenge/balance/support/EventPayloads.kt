package br.com.itau.challenge.balance.support

import java.util.UUID

/** Mensagens JSON do topico de entrada, no formato imposto pelo cliente (`transaction-event.schema.json`). */
object EventPayloads {
    const val DEFAULT_OWNER = "315e3cfe-f4af-4cd2-b298-a449e614349a"
    const val BASE_TIMESTAMP_MICROS = 1751749453433000L

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
        accountCreatedAtMicros: Long = 1634874339000000L,
    ): String =
        """{"transaction":{"id":"$transactionId","type":"$transactionType","amount":$transactionAmount,"currency":"$currency",""" +
            """"status":"$transactionStatus","timestamp":$timestampMicros},"account":{"id":"$accountId","owner":"$ownerId",""" +
            """"created_at":$accountCreatedAtMicros,"status":"$accountStatus","balance":{"amount":$balanceAmount,"currency":"$currency"}}}"""
}
