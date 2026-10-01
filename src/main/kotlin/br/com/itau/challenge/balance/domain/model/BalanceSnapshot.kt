package br.com.itau.challenge.balance.domain.model

data class BalanceSnapshot(
    val accountId: AccountId,
    val ownerId: OwnerId,
    val status: AccountStatus,
    val balance: Money,
    val accountCreatedAt: EventInstant,
    val precedence: Precedence,
) {
    fun isDisabled(): Boolean = status == AccountStatus.DISABLED

    fun supersedes(current: BalanceSnapshot?): Boolean = current == null || precedence > current.precedence

    override fun toString(): String = "BalanceSnapshot(accountId=$accountId, status=$status, precedence=$precedence)"

    companion object {
        fun from(event: TransactionEvent): BalanceSnapshot =
            BalanceSnapshot(
                accountId = event.account.id,
                ownerId = event.account.owner,
                status = event.account.status,
                balance = event.account.balance,
                accountCreatedAt = event.account.createdAt,
                precedence = Precedence(event.transaction.timestamp, event.transaction.id),
            )
    }
}
