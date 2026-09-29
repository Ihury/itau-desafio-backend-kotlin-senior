package br.com.itau.challenge.balance

import br.com.itau.challenge.balance.application.ProcessTransactionEventService
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.AccountStatus
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.TransactionStatus
import br.com.itau.challenge.balance.testing.ConvergenceModel
import br.com.itau.challenge.balance.testing.ConvergenceModel.EventSpec
import br.com.itau.challenge.balance.testing.InMemoryBalanceStore
import br.com.itau.challenge.balance.testing.NaiveLastWriteWinsStore
import br.com.itau.challenge.balance.testing.RecordingProcessingMetrics
import br.com.itau.challenge.balance.testing.StoreUnderTest
import io.kotest.property.PropTestConfig
import io.kotest.property.Arb
import io.kotest.property.arbitrary.list
import io.kotest.property.checkAll
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.util.Random
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
class ConvergencePropertyTest {
    private companion object {
        const val SEED = 20260929L
        const val ITERATIONS = 1000
        val CONFIG = PropTestConfig(seed = SEED, iterations = ITERATIONS)
    }

    private fun inMemory(): StoreUnderTest {
        val store = InMemoryBalanceStore()
        return StoreUnderTest(store) { store.current(it) }
    }

    private fun naive(): StoreUnderTest {
        val store = NaiveLastWriteWinsStore()
        return StoreUnderTest(store) { store.current(it) }
    }

    /** Entregas alternativas do MESMO conjunto de eventos: ordem dada, inversa, crescente, decrescente, embaralhadas e dobrada. */
    private fun deliveries(specs: List<EventSpec>): List<List<EventSpec>> {
        val ascending = specs.sortedWith(compareBy<EventSpec> { it.accountId }.thenBy { it.timestampMicros }.thenBy { it.transactionId })
        val shuffles = (1L..4L).map { specs.shuffled(Random(it * 7919 + specs.size)) }
        val doubled = (specs + specs).shuffled(Random(specs.size.toLong()))
        return listOf(specs, specs.reversed(), ascending, ascending.reversed(), specs + specs) + shuffles + listOf(doubled)
    }

    private class Coverage {
        val enabled = AtomicInteger()
        val disabled = AtomicInteger()
        val approved = AtomicInteger()
        val declined = AtomicInteger()
        val ties = AtomicInteger()
        val sameTxDifferentAccounts = AtomicInteger()
        val duplicates = AtomicInteger()
    }

    private val coverage = Coverage()

    private fun observe(specs: List<EventSpec>) {
        specs.forEach {
            val content = ConvergenceModel.contentOf(it)
            if (content.accountStatus == AccountStatus.ENABLED) coverage.enabled.incrementAndGet() else coverage.disabled.incrementAndGet()
            if (content.transactionStatus == TransactionStatus.APPROVED) coverage.approved.incrementAndGet() else coverage.declined.incrementAndGet()
        }
        val keys = specs.map { it.accountId to it.key }
        if (keys.size != keys.toSet().size) coverage.duplicates.incrementAndGet()
        if (specs.groupBy { it.accountId to it.timestampMicros }.values.any { group -> group.map { it.transactionId }.toSet().size > 1 }) coverage.ties.incrementAndGet()
        if (specs.groupBy { it.transactionId }.values.any { group -> group.map { it.accountId }.toSet().size > 1 }) coverage.sameTxDifferentAccounts.incrementAndGet()
    }

    /**
     * Aplica [order] a um armazenamento novo e confere: (1) o snapshot final de cada conta e o do oraculo; (2, se
     * [checkOutcomes]) cada evento produz exatamente o desfecho previsto pelo modelo de referencia (`Applied` se supera o
     * vigente, `Duplicate(false)` se a chave e igual, `Obsolete` se inferior) e o servico contabiliza um desfecho por evento.
     */
    private fun assertConverges(
        factory: () -> StoreUnderTest,
        all: List<EventSpec>,
        order: List<EventSpec>,
        checkOutcomes: Boolean,
    ) {
        val store = factory()
        val metrics = RecordingProcessingMetrics()
        val service = ProcessTransactionEventService(store.writer, metrics)
        val maxSoFar = HashMap<String, EventSpec>()
        val results = order.map { spec -> spec to service.process(spec.toEvent()) }

        all.map { it.accountId }.toSet().forEach { account ->
            val oracle = assertNotNull(ConvergenceModel.winnerOf(all.filter { it.accountId == account }))
            assertEquals(oracle.toSnapshot(), store.current(AccountId.parse(account)), "snapshot final da conta $account apos ${order.map { it.key.first - ConvergenceModel.BASE_TIMESTAMP_MICROS to it.txIndex }}")
        }
        if (!checkOutcomes) return
        results.forEach { (spec, result) ->
            val current = maxSoFar[spec.accountId]
            val expected =
                when {
                    current == null || compareValues(spec.timestampMicros, current.timestampMicros).let { if (it != 0) it > 0 else spec.transactionId > current.transactionId } -> ApplyResult.Applied
                    spec.key == current.key -> ApplyResult.Duplicate(conflicting = false)
                    else -> ApplyResult.Obsolete
                }
            assertEquals(expected, result, "desfecho de ${spec.key}")
            if (expected == ApplyResult.Applied) maxSoFar[spec.accountId] = spec
        }
        assertEquals(order.size, metrics.outcomes.size, "um desfecho contabilizado por evento entregue")
        assertTrue(metrics.outcomes.none { it == "duplicate(conflicting)" }, "conteudo derivado da chave nunca diverge")
    }

    private fun runProperty(
        factory: () -> StoreUnderTest,
        accounts: Int,
        checkOutcomes: Boolean = true,
    ) = runBlocking {
        checkAll(CONFIG, Arb.list(ConvergenceModel.eventSpecs(accounts), 1..30)) { specs ->
            observe(specs)
            deliveries(specs).forEach { order -> assertConverges(factory, specs, order, checkOutcomes) }
        }
    }

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
        val context = runProperty(::inMemory, accounts = 1)

        assertEquals(ITERATIONS, context.attempts())
        assertTrue(coverage.enabled.get() > 0 && coverage.disabled.get() > 0, "ENABLED e DISABLED exercitados")
        assertTrue(coverage.approved.get() > 0 && coverage.declined.get() > 0, "APPROVED e DECLINED exercitados")
        assertTrue(coverage.ties.get() > 0, "empates de timestamp exercitados")
        assertTrue(coverage.duplicates.get() > 0, "duplicatas exercitadas")
    }

    @Test
    fun `two interleaved accounts never interfere, even with the same transaction id`() {
        val context = runProperty(::inMemory, accounts = 2)

        assertEquals(ITERATIONS, context.attempts())
        assertTrue(coverage.sameTxDifferentAccounts.get() > 0, "mesmo transactionId em contas diferentes exercitado")
        assertTrue(coverage.ties.get() > 0)
    }

    @Test
    fun `the property fails against a last write wins store, proving it detects order dependence`() {
        val failure = assertFailsWith<AssertionError> { runProperty(::naive, accounts = 1) }

        val message = assertNotNull(failure.message)
        assertTrue("Property failed" in message || "Shrunk" in message || "Attempt" in message, message)
        println("CONTRAEXEMPLO (propriedade completa contra o store ingenuo):\n$message")
    }

    @Test
    fun `the convergence assertion alone also fails against a last write wins store and shrinks to the minimal order`() {
        val failure = assertFailsWith<AssertionError> { runProperty(::naive, accounts = 1, checkOutcomes = false) }

        val message = assertNotNull(failure.message)
        println("CONTRAEXEMPLO (somente convergencia do snapshot final contra o store ingenuo):\n$message")
    }
}
