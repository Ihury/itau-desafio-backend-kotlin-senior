package br.com.itau.challenge.balance.domain.exception

/** O armazenamento rejeitou a escrita (ex.: validacao); nao e retentavel. */
class BalanceStoreRejectedException(
    cause: Throwable? = null,
) : RuntimeException("balance store rejected the write", cause)
