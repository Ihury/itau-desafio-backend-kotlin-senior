package br.com.itau.challenge.balance.config

import br.com.itau.challenge.balance.adapter.output.dynamodb.readCircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics
import io.micrometer.core.instrument.binder.MeterBinder
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * Circuit breaker programatico da leitura (Resilience4j core, sem starter Spring): registry, breaker `dynamodb-read` e
 * binding das metricas `resilience4j.circuitbreaker.*` no Micrometer (todo `MeterBinder` e ligado pelo Spring Boot).
 */
@Configuration
@EnableConfigurationProperties(CircuitBreakerProperties::class)
class ResilienceConfig {
    @Bean
    fun circuitBreakerRegistry(properties: CircuitBreakerProperties): CircuitBreakerRegistry =
        CircuitBreakerRegistry.of(
            readCircuitBreakerConfig(
                window = properties.window,
                minCalls = properties.minCalls,
                failureRate = properties.failureRate,
                slowCall = properties.slowCall,
                slowRate = properties.slowRate,
                openWait = properties.openWait,
                halfOpenCalls = properties.halfOpenCalls,
            ),
        )

    @Bean
    fun dynamoDbReadCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker = registry.circuitBreaker(DYNAMODB_READ)

    @Bean
    fun circuitBreakerMetrics(registry: CircuitBreakerRegistry): MeterBinder = TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry)

    private companion object {
        const val DYNAMODB_READ = "dynamodb-read"
    }
}
