package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.exception.BalanceStoreCircuitOpenException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotReader
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig

fun readCircuitBreakerConfig(properties: CircuitBreakerProperties): CircuitBreakerConfig =
    CircuitBreakerConfig
        .custom()
        .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
        .slidingWindowSize(properties.window.seconds.toInt().coerceAtLeast(1))
        .minimumNumberOfCalls(properties.minCalls)
        .failureRateThreshold(properties.failureRate)
        .slowCallDurationThreshold(properties.slowCall)
        .slowCallRateThreshold(properties.slowRate)
        .waitDurationInOpenState(properties.openWait)
        .automaticTransitionFromOpenToHalfOpenEnabled(true)
        .permittedNumberOfCallsInHalfOpenState(properties.halfOpenCalls)
        .recordExceptions(BalanceStoreUnavailableException::class.java)
        .writableStackTraceEnabled(false)
        .build()

class CircuitBreakingBalanceSnapshotReader(
    private val delegate: BalanceSnapshotReader,
    private val circuitBreaker: CircuitBreaker,
) : BalanceSnapshotReader {
    override fun find(accountId: AccountId): BalanceSnapshot? =
        try {
            circuitBreaker.executeSupplier { delegate.find(accountId) }
        } catch (rejected: CallNotPermittedException) {
            throw BalanceStoreCircuitOpenException(rejected)
        }
}
