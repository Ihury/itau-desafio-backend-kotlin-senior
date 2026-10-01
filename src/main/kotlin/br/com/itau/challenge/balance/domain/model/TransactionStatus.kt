package br.com.itau.challenge.balance.domain.model

enum class TransactionStatus {
    APPROVED,
    DECLINED,
    ;

    companion object {
        fun parse(raw: String): TransactionStatus = parseEnum(raw)
    }
}
