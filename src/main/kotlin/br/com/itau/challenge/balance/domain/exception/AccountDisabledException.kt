package br.com.itau.challenge.balance.domain.exception

import br.com.itau.challenge.balance.domain.model.AccountId

/** A conta consultada esta desabilitada: nenhum saldo e exposto. Carrega somente o identificador. */
class AccountDisabledException(
    val accountId: AccountId,
) : RuntimeException("account disabled: $accountId")
