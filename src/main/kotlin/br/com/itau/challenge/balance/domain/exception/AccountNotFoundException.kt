package br.com.itau.challenge.balance.domain.exception

import br.com.itau.challenge.balance.domain.model.AccountId

/** Nao ha snapshot para a conta consultada. Carrega somente o identificador. */
class AccountNotFoundException(
    val accountId: AccountId,
) : RuntimeException("account not found: $accountId")
