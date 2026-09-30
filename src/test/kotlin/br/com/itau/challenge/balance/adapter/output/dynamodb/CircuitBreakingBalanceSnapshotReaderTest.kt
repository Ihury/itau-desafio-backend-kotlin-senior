package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.exception.BalanceStoreCircuitOpenException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.domain.model.TransactionEventFixtures.DEFAULT_ACCOUNT_ID
import br.com.itau.challenge.balance.domain.model.TransactionEventFixtures.transactionEvent
import br.com.itau.challenge.balance.port.output.BalanceSnapshotReader
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreaker.State
import org.awaitility.kotlin.await
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame

class CircuitBreakingBalanceSnapshotReaderTest {
    private val accountId = AccountId.parse(DEFAULT_ACCOUNT_ID)
    private val snapshot = BalanceSnapshot.from(transactionEvent())

    /** Delegate falso: cada chamada executa [behavior] e conta a invocacao. */
    private class FakeReader(
        var behavior: () -> BalanceSnapshot? = { null },
    ) : BalanceSnapshotReader {
        val calls = AtomicInteger()

        override fun find(accountId: AccountId): BalanceSnapshot? {
            calls.incrementAndGet()
            return behavior()
        }
    }

    private fun breaker(
        minCalls: Int = 4,
        failureRate: Float = 50f,
        slowCall: Duration = Duration.ofSeconds(5),
        slowRate: Float = 80f,
        openWait: Duration = Duration.ofSeconds(30),
        halfOpenCalls: Int = 2,
    ): CircuitBreaker =
        CircuitBreaker.of(
            "test",
            readCircuitBreakerConfig(
                slidingWindow = Duration.ofSeconds(10),
                minCalls = minCalls,
                failureRateThresholdPercent = failureRate,
                slowCallThreshold = slowCall,
                slowCallRateThresholdPercent = slowRate,
                openWait = openWait,
                halfOpenCalls = halfOpenCalls,
            ),
        )

    private fun unavailable() = BalanceStoreUnavailableException(StoreFailureCause.UNAVAILABLE)

    private fun repeatFind(
        reader: BalanceSnapshotReader,
        times: Int,
    ) = repeat(times) { runCatching { reader.find(accountId) } }

    @Test
    fun `a rejection by the open circuit is a specific store unavailable without stack trace, still unavailable for the callers`() {
        val circuit = breaker()
        val reader = CircuitBreakingBalanceSnapshotReader(FakeReader { throw unavailable() }, circuit)
        repeatFind(reader, 4)

        val rejection = assertFailsWith<BalanceStoreCircuitOpenException> { reader.find(accountId) }

        assertIs<BalanceStoreUnavailableException>(rejection)
        assertEquals(StoreFailureCause.UNAVAILABLE, rejection.failureCause)
        assertIs<CallNotPermittedException>(rejection.cause)
        assertEquals(0, rejection.stackTrace.size, "sem pilha")
        assertEquals(0, rejection.cause!!.stackTrace.size, "a CallNotPermittedException tambem nao preenche pilha")
    }

    @Test
    fun `a real failure of the store keeps its own stack trace and is not a circuit rejection`() {
        val reader = CircuitBreakingBalanceSnapshotReader(FakeReader { throw unavailable() }, breaker())

        val failure = assertFailsWith<BalanceStoreUnavailableException> { reader.find(accountId) }

        assertFalse(failure is BalanceStoreCircuitOpenException)
    }

    @Test
    fun `failures above the threshold open the circuit and the next call fails fast without hitting the delegate`() {
        val delegate = FakeReader { throw unavailable() }
        val circuit = breaker()
        val reader = CircuitBreakingBalanceSnapshotReader(delegate, circuit)

        repeatFind(reader, 4)

        assertEquals(State.OPEN, circuit.state)
        val failure = assertFailsWith<BalanceStoreUnavailableException> { reader.find(accountId) }
        assertEquals(StoreFailureCause.UNAVAILABLE, failure.failureCause)
        assertIs<CallNotPermittedException>(failure.cause)
        assertEquals(4, delegate.calls.get(), "com o circuito aberto o delegate nao e invocado")
    }

    @Test
    fun `the original unavailable exception passes through untouched while the circuit is closed`() {
        val original = BalanceStoreUnavailableException(StoreFailureCause.THROTTLED)
        val reader = CircuitBreakingBalanceSnapshotReader(FakeReader { throw original }, breaker())

        assertSame(original, assertFailsWith<BalanceStoreUnavailableException> { reader.find(accountId) })
    }

    @Test
    fun `found snapshots and not found both count as success`() {
        val delegate = FakeReader()
        val circuit = breaker()
        val reader = CircuitBreakingBalanceSnapshotReader(delegate, circuit)

        delegate.behavior = { snapshot }
        assertSame(snapshot, reader.find(accountId))
        delegate.behavior = { null }
        assertNull(reader.find(accountId))
        repeatFind(reader, 10)

        assertEquals(State.CLOSED, circuit.state)
        assertEquals(0, circuit.metrics.numberOfFailedCalls)
        assertEquals(12, circuit.metrics.numberOfSuccessfulCalls)
    }

    @Test
    fun `failure rate below the threshold keeps the circuit closed`() {
        val delegate = FakeReader()
        val circuit = breaker()
        val reader = CircuitBreakingBalanceSnapshotReader(delegate, circuit)

        repeat(3) { runCatching { reader.find(accountId) } }
        delegate.behavior = { throw unavailable() }
        runCatching { reader.find(accountId) }

        assertEquals(State.CLOSED, circuit.state)
        assertEquals(1, circuit.metrics.numberOfFailedCalls)
    }

    @Test
    fun `other exceptions do not count as failures and propagate unchanged`() {
        val corrupted = IllegalStateException("corrupted balance item: invalid attribute 'ownerId'")
        val circuit = breaker()
        val reader = CircuitBreakingBalanceSnapshotReader(FakeReader { throw corrupted }, circuit)

        repeat(10) { assertSame(corrupted, assertFailsWith<IllegalStateException> { reader.find(accountId) }) }

        assertEquals(State.CLOSED, circuit.state)
        assertEquals(0, circuit.metrics.numberOfFailedCalls)
    }

    @Test
    fun `slow calls in a proportion at or above the limit also open the circuit`() {
        val delegate = FakeReader { Thread.sleep(40).let { snapshot } }
        val circuit = breaker(slowCall = Duration.ofMillis(10), slowRate = 80f)
        val reader = CircuitBreakingBalanceSnapshotReader(delegate, circuit)

        repeatFind(reader, 4)

        assertEquals(State.OPEN, circuit.state)
        assertEquals(0, circuit.metrics.numberOfFailedCalls, "as chamadas lentas tiveram sucesso: abriu so pela taxa de lentas")
    }

    @Test
    fun `open moves to half open by itself after the wait and enough successful test calls close it`() {
        val delegate = FakeReader { throw unavailable() }
        val circuit = breaker(openWait = Duration.ofMillis(150))
        val reader = CircuitBreakingBalanceSnapshotReader(delegate, circuit)
        repeatFind(reader, 4)
        assertEquals(State.OPEN, circuit.state)

        await.atMost(Duration.ofSeconds(3)).until { circuit.state == State.HALF_OPEN }
        delegate.behavior = { snapshot }
        repeatFind(reader, 2)

        assertEquals(State.CLOSED, circuit.state)
    }

    @Test
    fun `a failing test call in half open reopens the circuit`() {
        val delegate = FakeReader { throw unavailable() }
        val circuit = breaker(openWait = Duration.ofMillis(150))
        val reader = CircuitBreakingBalanceSnapshotReader(delegate, circuit)
        repeatFind(reader, 4)
        await.atMost(Duration.ofSeconds(3)).until { circuit.state == State.HALF_OPEN }

        repeatFind(reader, 2)

        assertEquals(State.OPEN, circuit.state)
    }
}
