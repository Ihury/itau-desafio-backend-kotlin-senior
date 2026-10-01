package br.com.itau.challenge.balance.domain.exception

import br.com.itau.challenge.balance.domain.model.AccountId

class AccountDisabledException(
    val accountId: AccountId,
) : RuntimeException("account disabled: $accountId")
