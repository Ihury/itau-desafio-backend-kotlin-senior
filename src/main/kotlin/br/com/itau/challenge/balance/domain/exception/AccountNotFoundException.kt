package br.com.itau.challenge.balance.domain.exception

import br.com.itau.challenge.balance.domain.model.AccountId

class AccountNotFoundException(
    val accountId: AccountId,
) : RuntimeException("account not found: $accountId")
