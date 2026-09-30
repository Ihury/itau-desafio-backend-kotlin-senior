package br.com.itau.challenge.balance.domain.model

data class TransactionEvent(
    val transaction: Transaction,
    val account: AccountState,
)
