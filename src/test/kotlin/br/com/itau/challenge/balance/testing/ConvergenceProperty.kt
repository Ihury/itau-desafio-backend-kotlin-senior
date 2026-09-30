package br.com.itau.challenge.balance.testing

import br.com.itau.challenge.balance.application.FutureTolerance
import br.com.itau.challenge.balance.application.ProcessTransactionEventService
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.testing.ConvergenceModel.EventSpec
import io.kotest.property.Arb
import io.kotest.property.PropTestConfig
import io.kotest.property.PropertyContext
import io.kotest.property.arbitrary.list
import io.kotest.property.checkAll
import kotlinx.coroutines.runBlocking
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.Random
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Nucleo da propriedade de convergencia, compartilhado pelo teste unitario (fake em memoria) e pelo de integracao (DynamoDB
 * Local): a MESMA propriedade roda contra qualquer [StoreUnderTest]. Para qualquer lista de 1 a 30 eventos e para varias
 * entregas alternativas do mesmo conjunto (ordem dada, inversa, crescente, decrescente, embaralhadas e dobrada), o snapshot
 * final de cada conta e o do evento de maior `(timestamp, transactionId)` e cada evento produz o desfecho previsto.
 */
object ConvergenceProperty {
    /** Relogio fixo depois de todos os eventos gerados (base em 2025): a tolerancia de futuro nunca interfere na propriedade. */
    private val CLOCK: Clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC)

    /** Um armazenamento pronto para uma entrega e o remapeamento das contas do modelo (identidade, ou contas aleatorias em tabela compartilhada). */
    class Scenario(
        val store: StoreUnderTest,
        val remap: (EventSpec) -> EventSpec = { it },
    )

    /** Entregas alternativas do MESMO conjunto de eventos. */
    fun deliveries(specs: List<EventSpec>): List<List<EventSpec>> {
        val ascending = specs.sortedWith(compareBy<EventSpec> { it.accountId }.thenBy { it.timestampMicros }.thenBy { it.transactionId })
        val shuffles = (1L..4L).map { specs.shuffled(Random(it * 7919 + specs.size)) }
        val doubled = (specs + specs).shuffled(Random(specs.size.toLong()))
        return listOf(specs, specs.reversed(), ascending, ascending.reversed(), specs + specs) + shuffles + listOf(doubled)
    }

    /**
     * Aplica [order] ao [Scenario] e confere: (1) o snapshot final de cada conta e o do oraculo; (2, se [checkOutcomes]) cada
     * evento produz exatamente o desfecho do modelo de referencia (`Applied` se supera o vigente, `Duplicate(false)` se a chave
     * e igual, `Obsolete` se inferior) e o servico contabiliza um desfecho por evento, nunca `conflicting`.
     */
    fun assertConverges(
        scenario: Scenario,
        all: List<EventSpec>,
        order: List<EventSpec>,
        checkOutcomes: Boolean,
    ) {
        val mappedAll = all.map(scenario.remap)
        val mappedOrder = order.map(scenario.remap)
        val metrics = RecordingProcessingMetrics()
        val service = ProcessTransactionEventService(scenario.store.writer, metrics, CLOCK, FutureTolerance(Duration.ofMinutes(5)))
        val results = mappedOrder.map { spec -> spec to service.process(spec.toEvent()) }

        mappedAll.map { it.accountId }.toSet().forEach { account ->
            val oracle = assertNotNull(ConvergenceModel.winnerOf(mappedAll.filter { it.accountId == account }))
            assertEquals(
                oracle.toSnapshot(),
                scenario.store.current(AccountId.parse(account)),
                "snapshot final da conta $account apos ${order.map { it.timestampOffsetMicros to it.transactionIdIndex }}",
            )
        }
        if (!checkOutcomes) return
        val winnerSoFar = HashMap<String, EventSpec>()
        results.forEach { (spec, result) ->
            val current = winnerSoFar[spec.accountId]
            val expected =
                when {
                    current == null || isSuperseding(spec, current) -> ApplyResult.Applied
                    spec.key == current.key -> ApplyResult.Duplicate(conflicting = false)
                    else -> ApplyResult.Obsolete
                }
            assertEquals(expected, result, "desfecho de ${spec.key}")
            if (expected == ApplyResult.Applied) winnerSoFar[spec.accountId] = spec
        }
        assertEquals(order.size, metrics.outcomes.size, "um desfecho contabilizado por evento entregue")
        assertTrue(metrics.outcomes.none { it == "duplicate(conflicting)" }, "conteudo derivado da chave nunca diverge")
    }

    private fun isSuperseding(
        next: EventSpec,
        current: EventSpec,
    ): Boolean =
        if (next.timestampMicros != current.timestampMicros) {
            next.timestampMicros > current.timestampMicros
        } else {
            next.transactionId > current.transactionId
        }

    /** Executa a propriedade com `checkAll`; [onCase] observa cada lista gerada (cobertura da geracao). */
    fun run(
        config: PropTestConfig,
        accounts: Int,
        checkOutcomes: Boolean = true,
        onCase: (List<EventSpec>) -> Unit = {},
        newScenario: () -> Scenario,
    ): PropertyContext =
        runBlocking {
            checkAll(config, Arb.list(ConvergenceModel.eventSpecs(accounts), 1..30)) { specs ->
                onCase(specs)
                deliveries(specs).forEach { order -> assertConverges(newScenario(), specs, order, checkOutcomes) }
            }
        }

    /** Conta aleatoria nova para cada indice de conta do modelo (tabela compartilhada). */
    fun randomAccounts(): (EventSpec) -> EventSpec {
        val mapping = HashMap<Int, String>()
        return { spec -> spec.withAccount(mapping.getOrPut(spec.accountIndex) { UUID.randomUUID().toString() }) }
    }
}
