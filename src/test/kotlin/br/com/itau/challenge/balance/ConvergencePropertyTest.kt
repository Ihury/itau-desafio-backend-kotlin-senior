package br.com.itau.challenge.balance

import br.com.itau.challenge.balance.domain.model.AccountStatus
import br.com.itau.challenge.balance.domain.model.TransactionStatus
import br.com.itau.challenge.balance.testing.ConvergenceModel
import br.com.itau.challenge.balance.testing.ConvergenceModel.EventSpec
import br.com.itau.challenge.balance.testing.ConvergenceProperty
import br.com.itau.challenge.balance.testing.InMemoryBalanceStore
import br.com.itau.challenge.balance.testing.NaiveLastWriteWinsStore
import br.com.itau.challenge.balance.testing.StoreUnderTest
import io.kotest.common.ExperimentalKotest
import io.kotest.property.PropTestConfig
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@OptIn(ExperimentalKotest::class)
class ConvergencePropertyTest {
    private companion object {
        const val SEED = 20260929L
        const val ITERATIONS = 1000
        val CONFIG = PropTestConfig(seed = SEED, iterations = ITERATIONS)
    }

    private fun inMemoryScenario(): ConvergenceProperty.Scenario {
        val store = InMemoryBalanceStore()
        return ConvergenceProperty.Scenario(StoreUnderTest(store) { store.peek(it) })
    }

    private fun naiveScenario(): ConvergenceProperty.Scenario {
        val store = NaiveLastWriteWinsStore()
        return ConvergenceProperty.Scenario(StoreUnderTest(store) { store.peek(it) })
    }

    private class GenerationCoverage {
        val enabled = AtomicInteger()
        val disabled = AtomicInteger()
        val approved = AtomicInteger()
        val declined = AtomicInteger()
        val ties = AtomicInteger()
        val sameTxDifferentAccounts = AtomicInteger()
        val duplicates = AtomicInteger()
        val upperCase = AtomicInteger()

        fun observe(specs: List<EventSpec>) {
            specs.forEach {
                val content = ConvergenceModel.derivedContentOf(it)
                if (content.accountStatus == AccountStatus.ENABLED) enabled.incrementAndGet() else disabled.incrementAndGet()
                if (content.transactionStatus == TransactionStatus.APPROVED) approved.incrementAndGet() else declined.incrementAndGet()
                if (it.uppercaseTransactionId) upperCase.incrementAndGet()
            }
            val keys = specs.map { it.accountId to it.precedenceKey }
            if (keys.size != keys.toSet().size) duplicates.incrementAndGet()
            if (specs.groupBy { it.accountId to it.timestampMicros }.values.any { group -> group.map { it.transactionId }.toSet().size > 1 }) ties.incrementAndGet()
            if (specs.groupBy { it.transactionId }.values.any { group -> group.map { it.accountId }.toSet().size > 1 }) sameTxDifferentAccounts.incrementAndGet()
        }
    }

    private val generationCoverage = GenerationCoverage()

    @Test
    fun `the oracle orders ids textually so ffffffff beats 00000000 at the same instant, which UUID compareTo would invert`() {
        assertTrue(ConvergenceModel.hasUuidCompareToDivergence())
        val low = EventSpec(0, 0, 0, false)
        val high = EventSpec(0, 0, 1, false)
        assertEquals(high, ConvergenceModel.winnerOf(listOf(high, low)))
        assertEquals(high, ConvergenceModel.winnerOf(listOf(low, high)))
    }

    @Test
    fun `any permutation, duplication and interleaving of one account converges to the highest precedence event`() {
        val context = ConvergenceProperty.run(CONFIG, accounts = 1, onGeneratedCase = generationCoverage::observe, newScenario = ::inMemoryScenario)

        assertEquals(ITERATIONS, context.attempts())
        assertTrue(generationCoverage.enabled.get() > 0 && generationCoverage.disabled.get() > 0, "ENABLED e DISABLED exercitados")
        assertTrue(generationCoverage.approved.get() > 0 && generationCoverage.declined.get() > 0, "APPROVED e DECLINED exercitados")
        assertTrue(generationCoverage.ties.get() > 0, "empates de timestamp exercitados")
        assertTrue(generationCoverage.duplicates.get() > 0, "duplicatas exercitadas")
        assertTrue(generationCoverage.upperCase.get() > 0, "ids em maiusculas exercitados")
    }

    @Test
    fun `two interleaved accounts never interfere, even with the same transaction id`() {
        val context = ConvergenceProperty.run(CONFIG, accounts = 2, onGeneratedCase = generationCoverage::observe, newScenario = ::inMemoryScenario)

        assertEquals(ITERATIONS, context.attempts())
        assertTrue(generationCoverage.sameTxDifferentAccounts.get() > 0, "mesmo transactionId em contas diferentes exercitado")
        assertTrue(generationCoverage.ties.get() > 0)
    }

    @Test
    fun `the property fails against a last write wins store, proving it detects order dependence`() {
        val failure = assertFailsWith<AssertionError> { ConvergenceProperty.run(CONFIG, accounts = 1, newScenario = ::naiveScenario) }

        val message = assertNotNull(failure.message)
        assertTrue("Property failed" in message, message)
        assertTrue("shrunk from" in message, "contraexemplo encolhido: $message")
    }

    @Test
    fun `the convergence assertion alone also fails against a last write wins store and shrinks to the minimal order`() {
        val failure = assertFailsWith<AssertionError> { ConvergenceProperty.run(CONFIG, accounts = 1, checkOutcomes = false, newScenario = ::naiveScenario) }

        val message = assertNotNull(failure.message)
        assertTrue("snapshot final" in message, message)
    }
}
