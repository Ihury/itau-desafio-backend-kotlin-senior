package br.com.itau.challenge.balance.port.input

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot

/** Consulta do saldo mais atual de uma conta. */
interface GetBalanceUseCase {
    /**
     * Devolve o snapshot vigente da conta.
     *
     * @throws br.com.itau.challenge.balance.domain.exception.AccountNotFoundException conta sem snapshot
     * @throws br.com.itau.challenge.balance.domain.exception.AccountDisabledException snapshot vigente DISABLED
     * @throws br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException armazenamento indisponivel
     */
    fun getBalance(accountId: AccountId): BalanceSnapshot
}
