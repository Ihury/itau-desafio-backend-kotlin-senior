package br.com.itau.challenge.balance.adapter.output.dynamodb

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("balance.circuit-breaker")
class CircuitBreakerProperties(
    val window: Duration,
    val minCalls: Int,
    val failureRate: Float,
    val slowCall: Duration,
    val slowRate: Float,
    val openWait: Duration,
    val halfOpenCalls: Int,
)
