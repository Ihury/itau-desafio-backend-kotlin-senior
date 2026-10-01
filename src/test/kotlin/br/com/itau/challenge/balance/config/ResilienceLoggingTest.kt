package br.com.itau.challenge.balance.config

import br.com.itau.challenge.balance.adapter.output.dynamodb.CircuitBreakingBalanceSnapshotReader
import br.com.itau.challenge.balance.adapter.output.dynamodb.readCircuitBreakerConfig
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.testing.LogCapture
import br.com.itau.challenge.balance.testing.ScriptedBalanceSnapshotReader
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_ACCOUNT_ID
import br.com.itau.challenge.balance.testing.testCircuitBreakerProperties
import ch.qos.logback.classic.Level
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ResilienceLoggingTest {
    private val accountId = AccountId.parse(DEFAULT_ACCOUNT_ID)

    @JvmField
    @RegisterExtension
    val logs = LogCapture(ResilienceConfig::class.java, Level.DEBUG)

    private fun newReadBreaker(): CircuitBreaker {
        val registry = CircuitBreakerRegistry.of(readCircuitBreakerConfig(testCircuitBreakerProperties()))
        return ResilienceConfig().dynamoDbReadCircuitBreaker(registry)
    }

    private fun warns() = logs.at(Level.WARN)

    @Test
    fun `an open circuit transition logs exactly one warn with the states and fifty rejections add none`() {
        val breaker = newReadBreaker()
        val failing = CircuitBreakingBalanceSnapshotReader(ScriptedBalanceSnapshotReader { throw BalanceStoreUnavailableException(StoreFailureCause.UNAVAILABLE) }, breaker)

        repeat(4) { runCatching { failing.find(accountId) } }

        assertEquals(CircuitBreaker.State.OPEN, breaker.state)
        val transition = warns().single()
        assertTrue("dynamodb-read" in transition.formattedMessage, transition.formattedMessage)
        assertTrue("CLOSED" in transition.formattedMessage && "OPEN" in transition.formattedMessage, transition.formattedMessage)

        repeat(50) { runCatching { failing.find(accountId) } }

        assertEquals(1, warns().size, "as rejeicoes com o circuito aberto nao geram WARN: ${logs.messages}")
    }

    @Test
    fun `each state transition is one warn`() {
        val breaker = newReadBreaker()

        breaker.transitionToOpenState()
        breaker.transitionToHalfOpenState()
        breaker.transitionToClosedState()

        assertEquals(3, warns().size)
        assertTrue(warns().all { it.throwableProxy == null })
    }
}
