package br.com.itau.challenge.balance.domain.model

/** Evento de transacao: entrada imutavel, unidade de consumo, precedencia e idempotencia. */
data class TransactionEvent(
    val transaction: Transaction,
    val account: AccountState,
)
