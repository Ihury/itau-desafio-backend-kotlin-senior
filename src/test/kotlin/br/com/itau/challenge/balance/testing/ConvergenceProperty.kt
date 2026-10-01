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

object ConvergenceProperty {
    private val CLOCK_AFTER_ALL_GENERATED_EVENTS: Clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC)

    class Scenario(
        val store: StoreUnderTest,
        val remap: (EventSpec) -> EventSpec = { it },
    )

    fun alternativeDeliveriesOf(specs: List<EventSpec>): List<List<EventSpec>> {
        val ascending = specs.sortedWith(compareBy<EventSpec> { it.accountId }.thenBy { it.timestampMicros }.thenBy { it.transactionId })
        val shuffles = (1L..4L).map { specs.shuffled(Random(it * 7919 + specs.size)) }
        val doubled = (specs + specs).shuffled(Random(specs.size.toLong()))
        return listOf(specs, specs.reversed(), ascending, ascending.reversed(), specs + specs) + shuffles + listOf(doubled)
    }

    fun assertConverges(
        scenario: Scenario,
        all: List<EventSpec>,
        order: List<EventSpec>,
        checkOutcomes: Boolean,
    ) {
        val mappedAll = all.map(scenario.remap)
        val mappedOrder = order.map(scenario.remap)
        val metrics = RecordingProcessingMetrics()
        val service = ProcessTransactionEventService(scenario.store.writer, metrics, CLOCK_AFTER_ALL_GENERATED_EVENTS, FutureTolerance(Duration.ofMinutes(5)))
        val results = mappedOrder.map { spec -> spec to service.process(spec.toEvent()) }

        mappedAll.map { it.accountId }.toSet().forEach { account ->
            val oracle = assertNotNull(ConvergenceModel.winnerOf(mappedAll.filter { it.accountId == account }))
            assertEquals(
                oracle.toSnapshot(),
                scenario.store.currentStoredSnapshot(AccountId.parse(account)),
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
                    spec.precedenceKey == current.precedenceKey -> ApplyResult.Duplicate(conflicting = false)
                    else -> ApplyResult.Obsolete
                }
            assertEquals(expected, result, "desfecho de ${spec.precedenceKey}")
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

    fun run(
        config: PropTestConfig,
        accounts: Int,
        checkOutcomes: Boolean = true,
        onGeneratedCase: (List<EventSpec>) -> Unit = {},
        newScenario: () -> Scenario,
    ): PropertyContext =
        runBlocking {
            checkAll(config, Arb.list(ConvergenceModel.eventSpecs(accounts), 1..30)) { specs ->
                onGeneratedCase(specs)
                alternativeDeliveriesOf(specs).forEach { order -> assertConverges(newScenario(), specs, order, checkOutcomes) }
            }
        }

    fun randomAccounts(): (EventSpec) -> EventSpec {
        val mapping = HashMap<Int, String>()
        return { spec -> spec.withAccount(mapping.getOrPut(spec.accountIndex) { UUID.randomUUID().toString() }) }
    }
}
