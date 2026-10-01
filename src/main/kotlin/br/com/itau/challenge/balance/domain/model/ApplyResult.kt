package br.com.itau.challenge.balance.domain.model

sealed interface ApplyResult {
    data object Applied : ApplyResult

    data object Obsolete : ApplyResult

    data class Duplicate(
        val conflicting: Boolean,
    ) : ApplyResult
}
