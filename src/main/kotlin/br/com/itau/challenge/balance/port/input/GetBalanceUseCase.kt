package br.com.itau.challenge.balance.port.input

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot

interface GetBalanceUseCase {
    fun getBalance(accountId: AccountId): BalanceSnapshot
}
