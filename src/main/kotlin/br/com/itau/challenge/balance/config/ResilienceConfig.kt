package br.com.itau.challenge.balance.config

import br.com.itau.challenge.balance.adapter.output.dynamodb.readCircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics
import io.micrometer.core.instrument.binder.MeterBinder
import org.slf4j.LoggerFactory
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
                slidingWindow = properties.window,
                minCalls = properties.minCalls,
                failureRateThresholdPercent = properties.failureRate,
                slowCallThreshold = properties.slowCall,
                slowCallRateThresholdPercent = properties.slowRate,
                openWait = properties.openWait,
                halfOpenCalls = properties.halfOpenCalls,
            ),
        )

    @Bean
    fun dynamoDbReadCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker =
        registry.circuitBreaker(DYNAMODB_READ_BREAKER_NAME).also { breaker ->
            // WARN so nas transicoes de estado (uma linha por transicao): com o circuito aberto as rejeicoes por requisicao nao logam.
            breaker.eventPublisher.onStateTransition { event ->
                log.warn("circuit breaker {} changed state {} -> {}", event.circuitBreakerName, event.stateTransition.fromState, event.stateTransition.toState)
            }
        }

    @Bean
    fun circuitBreakerMetrics(registry: CircuitBreakerRegistry): MeterBinder = TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry)

    private companion object {
        private val log = LoggerFactory.getLogger(ResilienceConfig::class.java)
        const val DYNAMODB_READ_BREAKER_NAME = "dynamodb-read"
    }
}
