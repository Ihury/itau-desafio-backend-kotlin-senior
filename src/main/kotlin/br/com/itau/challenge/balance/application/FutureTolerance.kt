package br.com.itau.challenge.balance.application

import java.time.Duration

/**
 * Tolerancia de timestamp no futuro (FR-012): um instante do evento alem de `agora + tolerancia` e invalido. Tipo simples da
 * `application` (a regra Konsist proibe `@Value` nesta camada); o bean nasce em `config` a partir de `balance.future-tolerance`.
 */
data class FutureTolerance(
    val duration: Duration,
) {
    init {
        require(!duration.isNegative) { "a tolerancia de futuro nao pode ser negativa" }
    }
}
