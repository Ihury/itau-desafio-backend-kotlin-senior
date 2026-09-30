package br.com.itau.challenge.balance.domain.model

/**
 * Ordem total por [timestamp] numerico e, no empate, por [transactionId] pela comparacao lexicografica da string canonica em
 * minusculas (a ordem do DynamoDB). Nao usa `UUID.compareTo` (compara longs com sinal e diverge da ordem textual). O horario de
 * processamento nunca participa.
 */
data class Precedence(
    val timestamp: EventInstant,
    val transactionId: TransactionId,
) : Comparable<Precedence> {
    override fun compareTo(other: Precedence): Int {
        val byTimestamp = timestamp.compareTo(other.timestamp)
        return if (byTimestamp != 0) byTimestamp else transactionId.value.compareTo(other.transactionId.value)
    }
}
