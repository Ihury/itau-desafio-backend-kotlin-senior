package br.com.itau.challenge.balance.testing

import br.com.itau.challenge.balance.adapter.output.dynamodb.CircuitBreakerProperties
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotReader
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

fun testCircuitBreakerProperties(
    minCalls: Int = 4,
    failureRate: Float = 50f,
    slowCall: Duration = Duration.ofSeconds(5),
    slowRate: Float = 80f,
    openWait: Duration = Duration.ofSeconds(30),
    halfOpenCalls: Int = 2,
): CircuitBreakerProperties =
    CircuitBreakerProperties(
        window = Duration.ofSeconds(10),
        minCalls = minCalls,
        failureRate = failureRate,
        slowCall = slowCall,
        slowRate = slowRate,
        openWait = openWait,
        halfOpenCalls = halfOpenCalls,
    )

class ScriptedBalanceSnapshotReader(
    var behavior: () -> BalanceSnapshot? = { null },
) : BalanceSnapshotReader {
    val calls = AtomicInteger()

    override fun find(accountId: AccountId): BalanceSnapshot? {
        calls.incrementAndGet()
        return behavior()
    }
}
