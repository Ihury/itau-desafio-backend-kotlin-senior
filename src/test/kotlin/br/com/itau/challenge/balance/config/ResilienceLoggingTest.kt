package br.com.itau.challenge.balance.config

import br.com.itau.challenge.balance.adapter.output.dynamodb.CircuitBreakingBalanceSnapshotReader
import br.com.itau.challenge.balance.adapter.output.dynamodb.readCircuitBreakerConfig
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.domain.model.TransactionEventFixtures.DEFAULT_ACCOUNT_ID
import br.com.itau.challenge.balance.port.output.BalanceSnapshotReader
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Ruido de log com o circuito aberto: WARN so nas transicoes de estado, nunca por requisicao rejeitada. */
class ResilienceLoggingTest {
    private val accountId = AccountId.parse(DEFAULT_ACCOUNT_ID)

    private val logger = LoggerFactory.getLogger(ResilienceConfig::class.java) as Logger
    private lateinit var appender: ListAppender<ILoggingEvent>
    private var originalLevel: Level? = null

    @BeforeEach
    fun captureLogs() {
        originalLevel = logger.level
        appender = ListAppender<ILoggingEvent>().apply { start() }
        logger.addAppender(appender)
        logger.level = Level.DEBUG
    }

    @AfterEach
    fun releaseLogs() {
        logger.detachAppender(appender)
        logger.level = originalLevel
    }

    private fun newReadBreaker(): CircuitBreaker {
        val registry =
            CircuitBreakerRegistry.of(
                readCircuitBreakerConfig(
                    slidingWindow = Duration.ofSeconds(10),
                    minCalls = 4,
                    failureRateThresholdPercent = 50f,
                    slowCallThreshold = Duration.ofSeconds(5),
                    slowCallRateThresholdPercent = 80f,
                    openWait = Duration.ofSeconds(30),
                    halfOpenCalls = 2,
                ),
            )
        return ResilienceConfig().dynamoDbReadCircuitBreaker(registry)
    }

    private fun warns() = appender.list.filter { it.level == Level.WARN }

    @Test
    fun `an open circuit transition logs exactly one warn with the states and fifty rejections add none`() {
        val breaker = newReadBreaker()
        val failing =
            CircuitBreakingBalanceSnapshotReader(
                object : BalanceSnapshotReader {
                    override fun find(accountId: AccountId): BalanceSnapshot? = throw BalanceStoreUnavailableException(StoreFailureCause.UNAVAILABLE)
                },
                breaker,
            )

        repeat(4) { runCatching { failing.find(accountId) } }

        assertEquals(CircuitBreaker.State.OPEN, breaker.state)
        val transition = warns().single()
        assertTrue("dynamodb-read" in transition.formattedMessage, transition.formattedMessage)
        assertTrue("CLOSED" in transition.formattedMessage && "OPEN" in transition.formattedMessage, transition.formattedMessage)

        repeat(50) { runCatching { failing.find(accountId) } }

        assertEquals(1, warns().size, "as rejeicoes com o circuito aberto nao geram WARN: ${appender.list.map { it.formattedMessage }}")
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
