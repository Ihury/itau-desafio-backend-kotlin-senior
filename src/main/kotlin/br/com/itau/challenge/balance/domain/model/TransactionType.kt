package br.com.itau.challenge.balance.domain.model

enum class TransactionType {
    CREDIT,
    DEBIT,
    ;

    companion object {
        fun parse(raw: String): TransactionType = parseEnum(raw)
    }
}
