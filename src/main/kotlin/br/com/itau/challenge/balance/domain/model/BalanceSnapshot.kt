package br.com.itau.challenge.balance.domain.model

/**
 * Projecao (nunca um calculo) do evento de maior [precedence]. Todos os campos vem do mesmo evento. Tipo, valor e situacao da
 * transacao nao sao guardados (sem padrao de acesso).
 */
data class BalanceSnapshot(
    val accountId: AccountId,
    val ownerId: OwnerId,
    val status: AccountStatus,
    val balance: Money,
    val accountCreatedAt: EventInstant,
    val precedence: Precedence,
) {
    /** Vence o [current] so com precedencia estritamente maior (ou se nao ha [current]). */
    fun supersedes(current: BalanceSnapshot?): Boolean = current == null || precedence > current.precedence

    /** Nunca expoe titular nem saldo. */
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
