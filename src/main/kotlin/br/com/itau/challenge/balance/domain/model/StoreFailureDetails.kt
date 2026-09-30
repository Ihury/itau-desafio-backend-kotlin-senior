package br.com.itau.challenge.balance.domain.model

/**
 * Diagnostico seguro para log de uma falha do armazenamento: a classe da excecao do SDK, o codigo de erro do servico e o
 * status HTTP, quando existem. Deliberadamente sem a mensagem livre do SDK (pode conter dados) e sem payload.
 */
data class StoreFailureDetails(
    val exceptionClass: String,
    val errorCode: String? = null,
    val statusCode: Int? = null,
) {
    override fun toString(): String =
        buildString {
            append("exception=").append(exceptionClass)
            errorCode?.let { append(" errorCode=").append(it) }
            statusCode?.let { append(" statusCode=").append(it) }
        }
}
