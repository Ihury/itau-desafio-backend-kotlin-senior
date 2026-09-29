package br.com.itau.challenge.balance.domain.model

import java.math.BigDecimal

/** Construtores de dados de teste do dominio (valores validos por padrao; sobrescreva so o que o teste observa). */
object TransactionEventFixtures {
    const val DEFAULT_ACCOUNT_ID = "5b19c8b6-0cc4-4c72-a989-0c2ee15fa975"
    const val DEFAULT_OWNER_ID = "315e3cfe-f4af-4cd2-b298-a449e614349a"
    const val DEFAULT_TRANSACTION_ID = "8e8ae808-b154-48b5-9f3e-553935cc4543"
    const val DEFAULT_TIMESTAMP_MICROS = 1751749453433000L

    @Suppress("LongParameterList")
    fun transactionEvent(
        transactionId: String = DEFAULT_TRANSACTION_ID,
        timestampMicros: Long = DEFAULT_TIMESTAMP_MICROS,
        transactionType: TransactionType = TransactionType.CREDIT,
        transactionStatus: TransactionStatus = TransactionStatus.APPROVED,
        transactionAmount: String = "97.07",
        transactionCurrency: String = "BRL",
        accountId: String = DEFAULT_ACCOUNT_ID,
        ownerId: String = DEFAULT_OWNER_ID,
        accountStatus: AccountStatus = AccountStatus.ENABLED,
        balanceAmount: String = "183.12",
        balanceCurrency: String = "BRL",
        accountCreatedAtMicros: Long = 1634874339000000L,
    ): TransactionEvent =
        TransactionEvent(
            transaction =
                Transaction(
                    id = TransactionId.parse(transactionId),
                    type = transactionType,
                    amount = BigDecimal(transactionAmount),
                    currency = CurrencyCode.parse(transactionCurrency),
                    status = transactionStatus,
                    timestamp = EventInstant.transactionTimestamp(timestampMicros),
                ),
            account =
                AccountState(
                    id = AccountId.parse(accountId),
                    owner = OwnerId.parse(ownerId),
                    createdAt = EventInstant.accountCreatedAt(accountCreatedAtMicros),
                    status = accountStatus,
                    balance = Money.of(BigDecimal(balanceAmount), CurrencyCode.parse(balanceCurrency)),
                ),
        )
}
