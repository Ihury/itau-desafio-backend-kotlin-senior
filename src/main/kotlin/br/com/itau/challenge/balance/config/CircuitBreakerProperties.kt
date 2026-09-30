package br.com.itau.challenge.balance.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration
import kotlin.math.ceil

@ConfigurationProperties("balance.circuit-breaker")
class CircuitBreakerProperties(
    val window: Duration,
    val minCalls: Int,
    val failureRate: Float,
    val slowCall: Duration,
    val slowRate: Float,
    val openWait: Duration,
    val halfOpenCalls: Int,
) {
    /** Valor de `Retry-After` do 503: a espera em OPEN em segundos inteiros (minimo 1). */
    val retryAfterSeconds: Long get() = maxOf(1L, ceil(openWait.toMillis() / MILLIS_PER_SECOND).toLong())

    private companion object {
        private const val MILLIS_PER_SECOND = 1000.0
    }
}
