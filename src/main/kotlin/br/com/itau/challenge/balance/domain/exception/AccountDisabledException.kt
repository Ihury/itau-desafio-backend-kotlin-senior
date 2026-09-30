package br.com.itau.challenge.balance.domain.exception

import br.com.itau.challenge.balance.domain.model.AccountId

/** Conta desabilitada: nenhum saldo e exposto. */
class AccountDisabledException(
    val accountId: AccountId,
) : RuntimeException("account disabled: $accountId")
