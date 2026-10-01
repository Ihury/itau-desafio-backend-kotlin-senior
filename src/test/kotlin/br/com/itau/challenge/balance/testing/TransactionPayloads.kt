package br.com.itau.challenge.balance.testing

object TransactionPayloads {
    val ABSENT_FIELD: String? = null
    const val JSON_NULL = "null"

    val validFieldsInDocumentedOrder: LinkedHashMap<String, String> =
        linkedMapOf(
            "transaction.id" to quoted("8e8ae808-b154-48b5-9f3e-553935cc4543"),
            "transaction.type" to quoted("CREDIT"),
            "transaction.amount" to "97.07",
            "transaction.currency" to quoted("BRL"),
            "transaction.status" to quoted("APPROVED"),
            "transaction.timestamp" to "1751641364589998",
            "account.id" to quoted("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975"),
            "account.owner" to quoted("315e3cfe-f4af-4cd2-b298-a449e614349a"),
            "account.created_at" to "1634874339000000",
            "account.status" to quoted("ENABLED"),
            "account.balance.amount" to "183.12",
            "account.balance.currency" to quoted("BRL"),
        )

    fun quoted(text: String): String = "\"$text\""

    fun json(
        overrides: Map<String, String?> = emptyMap(),
        extras: String = "",
    ): String {
        val fields = validFieldsInDocumentedOrder.toMutableMap<String, String?>().apply { putAll(overrides) }

        fun field(path: String): String? = fields[path]?.let { "\"${path.substringAfterLast('.')}\":$it" }

        fun obj(vararg paths: String): String = paths.mapNotNull { field(it) }.joinToString(",")
        val transaction = obj("transaction.id", "transaction.type", "transaction.amount", "transaction.currency", "transaction.status", "transaction.timestamp")
        val balance = obj("account.balance.amount", "account.balance.currency")
        val account = obj("account.id", "account.owner", "account.created_at", "account.status")
        return "{\"transaction\":{$transaction$extras},\"account\":{$account,\"balance\":{$balance}}}"
    }
}
