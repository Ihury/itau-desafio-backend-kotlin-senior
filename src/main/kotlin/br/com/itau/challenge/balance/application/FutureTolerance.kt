package br.com.itau.challenge.balance.application

import java.time.Duration

data class FutureTolerance(
    val duration: Duration,
) {
    init {
        require(!duration.isNegative) { "a tolerancia de futuro nao pode ser negativa" }
    }
}
