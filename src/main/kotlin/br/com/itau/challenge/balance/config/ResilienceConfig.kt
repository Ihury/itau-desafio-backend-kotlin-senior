package br.com.itau.challenge.balance.config

import br.com.itau.challenge.balance.adapter.output.dynamodb.CircuitBreakerProperties
import br.com.itau.challenge.balance.adapter.output.dynamodb.readCircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics
import io.micrometer.core.instrument.binder.MeterBinder
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@EnableConfigurationProperties(CircuitBreakerProperties::class)
class ResilienceConfig {
    @Bean
    fun circuitBreakerRegistry(properties: CircuitBreakerProperties): CircuitBreakerRegistry =
        CircuitBreakerRegistry.of(readCircuitBreakerConfig(properties))

    @Bean
    fun dynamoDbReadCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker =
        registry.circuitBreaker(DYNAMODB_READ_BREAKER_NAME).also(::warnOnStateTransition)

    @Bean
    fun circuitBreakerMetrics(registry: CircuitBreakerRegistry): MeterBinder = TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry)

    private fun warnOnStateTransition(breaker: CircuitBreaker) {
        breaker.eventPublisher.onStateTransition { event ->
            log.warn("circuit breaker {} changed state {} -> {}", event.circuitBreakerName, event.stateTransition.fromState, event.stateTransition.toState)
        }
    }

    private companion object {
        private val log = LoggerFactory.getLogger(ResilienceConfig::class.java)
        const val DYNAMODB_READ_BREAKER_NAME = "dynamodb-read"
    }
}
