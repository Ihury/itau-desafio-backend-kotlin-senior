package br.com.itau.challenge.balance.domain.model

data class Precedence(
    val timestamp: EventInstant,
    val transactionId: TransactionId,
) : Comparable<Precedence> {
    override fun compareTo(other: Precedence): Int {
        val byTimestamp = timestamp.compareTo(other.timestamp)
        return if (byTimestamp != 0) byTimestamp else transactionId.value.compareTo(other.transactionId.value)
    }
}
