package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.port.output.BalanceSnapshotReader
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import java.time.Duration

/**
 * Configuracao do circuit breaker da leitura: janela por tempo, abertura por taxa de falha ou de chamadas lentas,
 * transicao automatica OPEN -> HALF_OPEN. So [BalanceStoreUnavailableException] conta como falha: item encontrado,
 * ausente (`null`) e falhas internas como [IllegalStateException] (item corrompido: o banco respondeu) contam como
 * sucesso, pois nao indicam indisponibilidade do armazenamento.
 */
@Suppress("LongParameterList")
fun readCircuitBreakerConfig(
    window: Duration,
    minCalls: Int,
    failureRate: Float,
    slowCall: Duration,
    slowRate: Float,
    openWait: Duration,
    halfOpenCalls: Int,
): CircuitBreakerConfig =
    CircuitBreakerConfig
        .custom()
        .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
        .slidingWindowSize(window.seconds.toInt().coerceAtLeast(1))
        .minimumNumberOfCalls(minCalls)
        .failureRateThreshold(failureRate)
        .slowCallDurationThreshold(slowCall)
        .slowCallRateThreshold(slowRate)
        .waitDurationInOpenState(openWait)
        .automaticTransitionFromOpenToHalfOpenEnabled(true)
        .permittedNumberOfCallsInHalfOpenState(halfOpenCalls)
        .recordExceptions(BalanceStoreUnavailableException::class.java)
        .build()

/**
 * Decorator do [BalanceSnapshotReader] com circuit breaker (falha rapida, Constitution V). Com o circuito aberto a chamada
 * falha com [BalanceStoreUnavailableException] SEM invocar o delegate; jamais devolve `null` nem saldo presumido.
 */
class CircuitBreakingBalanceSnapshotReader(
    private val delegate: BalanceSnapshotReader,
    private val circuitBreaker: CircuitBreaker,
) : BalanceSnapshotReader {
    override fun find(accountId: AccountId): BalanceSnapshot? =
        try {
            circuitBreaker.executeSupplier { delegate.find(accountId) }
        } catch (rejected: CallNotPermittedException) {
            throw BalanceStoreUnavailableException(StoreFailureCause.UNAVAILABLE, rejected)
        }
}
