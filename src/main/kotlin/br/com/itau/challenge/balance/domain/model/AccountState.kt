package br.com.itau.challenge.balance.domain.model

data class AccountState(
    val id: AccountId,
    val owner: OwnerId,
    val createdAt: EventInstant,
    val status: AccountStatus,
    val balance: Money,
) {
    override fun toString(): String = "AccountState(id=$id, status=$status)"
}
