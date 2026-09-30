package br.com.itau.challenge.balance.domain.model

/** Estado da conta informado pelo evento. */
data class AccountState(
    val id: AccountId,
    val owner: OwnerId,
    val createdAt: EventInstant,
    val status: AccountStatus,
    val balance: Money,
) {
    /** Nunca expoe titular nem saldo. */
    override fun toString(): String = "AccountState(id=$id, status=$status)"
}
