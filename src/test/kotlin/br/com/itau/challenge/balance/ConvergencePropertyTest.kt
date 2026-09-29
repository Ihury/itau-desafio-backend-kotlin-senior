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

/**
 * Propriedade de convergencia (SC-003, SC-004, SC-005) sobre o fake em memoria: para QUALQUER lista de 1 a 30 eventos,
 * qualquer permutacao, duplicacao e intercalacao entrega o mesmo snapshot final por conta, igual ao evento de maior
 * `(timestamp, transactionId minusculo)`. O conteudo do evento e derivado da chave (mesma chave, mesmo conteudo). A `seed`
 * e fixa (reprodutivel). O meta-teste prova que a MESMA propriedade reprova uma implementacao "ultimo a chegar vence".
 */
@OptIn(ExperimentalKotest::class)
class ConvergencePropertyTest {
    private companion object {
        const val SEED = 20260929L
        const val ITERATIONS = 1000
        val CONFIG = PropTestConfig(seed = SEED, iterations = ITERATIONS)
    }

    private fun inMemory(): ConvergenceProperty.Scenario {
        val store = InMemoryBalanceStore()
        return ConvergenceProperty.Scenario(StoreUnderTest(store) { store.current(it) })
    }

    private fun naive(): ConvergenceProperty.Scenario {
        val store = NaiveLastWriteWinsStore()
        return ConvergenceProperty.Scenario(StoreUnderTest(store) { store.current(it) })
    }

    private class Coverage {
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
                val content = ConvergenceModel.contentOf(it)
                if (content.accountStatus == AccountStatus.ENABLED) enabled.incrementAndGet() else disabled.incrementAndGet()
                if (content.transactionStatus == TransactionStatus.APPROVED) approved.incrementAndGet() else declined.incrementAndGet()
                if (it.upperCaseTx) upperCase.incrementAndGet()
            }
            val keys = specs.map { it.accountId to it.key }
            if (keys.size != keys.toSet().size) duplicates.incrementAndGet()
            if (specs.groupBy { it.accountId to it.timestampMicros }.values.any { group -> group.map { it.transactionId }.toSet().size > 1 }) ties.incrementAndGet()
            if (specs.groupBy { it.transactionId }.values.any { group -> group.map { it.accountId }.toSet().size > 1 }) sameTxDifferentAccounts.incrementAndGet()
        }
    }

    private val coverage = Coverage()

    @Test
    fun `the oracle exercises the uuid compareTo trap`() {
        assertTrue(ConvergenceModel.hasUuidCompareToDivergence())
        // o oraculo escolhe pelo texto: ffffffff... supera 00000000... no mesmo instante (UUID.compareTo diria o contrario)
        val low = EventSpec(0, 0, 0, false)
        val high = EventSpec(0, 0, 1, false)
        assertEquals(high, ConvergenceModel.winnerOf(listOf(high, low)))
        assertEquals(high, ConvergenceModel.winnerOf(listOf(low, high)))
    }

    @Test
    fun `any permutation, duplication and interleaving of one account converges to the highest precedence event`() {
        val context = ConvergenceProperty.run(CONFIG, accounts = 1, onCase = coverage::observe, newScenario = ::inMemory)

        assertEquals(ITERATIONS, context.attempts())
        assertTrue(coverage.enabled.get() > 0 && coverage.disabled.get() > 0, "ENABLED e DISABLED exercitados")
        assertTrue(coverage.approved.get() > 0 && coverage.declined.get() > 0, "APPROVED e DECLINED exercitados")
        assertTrue(coverage.ties.get() > 0, "empates de timestamp exercitados")
        assertTrue(coverage.duplicates.get() > 0, "duplicatas exercitadas")
        assertTrue(coverage.upperCase.get() > 0, "ids em maiusculas exercitados")
    }

    @Test
    fun `two interleaved accounts never interfere, even with the same transaction id`() {
        val context = ConvergenceProperty.run(CONFIG, accounts = 2, onCase = coverage::observe, newScenario = ::inMemory)

        assertEquals(ITERATIONS, context.attempts())
        assertTrue(coverage.sameTxDifferentAccounts.get() > 0, "mesmo transactionId em contas diferentes exercitado")
        assertTrue(coverage.ties.get() > 0)
    }

    @Test
    fun `the property fails against a last write wins store, proving it detects order dependence`() {
        val failure = assertFailsWith<AssertionError> { ConvergenceProperty.run(CONFIG, accounts = 1, newScenario = ::naive) }

        val message = assertNotNull(failure.message)
        assertTrue("Property failed" in message, message)
        assertTrue("shrunk from" in message, "contraexemplo encolhido: $message")
        println("CONTRAEXEMPLO (propriedade completa contra o store ingenuo):\n$message")
    }

    @Test
    fun `the convergence assertion alone also fails against a last write wins store and shrinks to the minimal order`() {
        val failure = assertFailsWith<AssertionError> { ConvergenceProperty.run(CONFIG, accounts = 1, checkOutcomes = false, newScenario = ::naive) }

        val message = assertNotNull(failure.message)
        assertTrue("snapshot final" in message, message)
        println("CONTRAEXEMPLO (somente convergencia do snapshot final contra o store ingenuo):\n$message")
    }
}
