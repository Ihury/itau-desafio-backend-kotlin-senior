package br.com.itau.challenge.balance.domain.exception

class BalanceStoreRejectedException(
    cause: Throwable? = null,
) : RuntimeException("balance store rejected the write", cause)
