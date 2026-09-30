package br.com.itau.challenge.balance.domain.model

/**
 * Diagnostico de uma falha do armazenamento seguro para log: a classe da excecao do SDK, o codigo de erro do servico e o status HTTP,
 * quando existem. Deliberadamente SEM a mensagem livre do SDK (pode conter dados) e sem payload.
 */
data class StoreFailureDetails(
    val exceptionClass: String,
    val errorCode: String? = null,
    val statusCode: Int? = null,
) {
    /** `exception=<classe> errorCode=<codigo> statusCode=<status>`, omitindo o que nao existe. */
    override fun toString(): String =
        buildString {
            append("exception=").append(exceptionClass)
            errorCode?.let { append(" errorCode=").append(it) }
            statusCode?.let { append(" statusCode=").append(it) }
        }
}
