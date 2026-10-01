package br.com.itau.challenge.balance.domain.model

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
