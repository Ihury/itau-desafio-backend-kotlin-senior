package br.com.itau.challenge.balance.domain.exception

/**
 * O armazenamento rejeitou a escrita (ex.: validacao). Nao e retentavel; o consumer a trata como falha nao classificada.
 */
class BalanceStoreRejectedException(
    cause: Throwable? = null,
) : RuntimeException("balance store rejected the write", cause)
