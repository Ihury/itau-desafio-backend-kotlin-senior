package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.support.ComposeControl
import br.com.itau.challenge.balance.support.DynamoDbTestSupport
import br.com.itau.challenge.balance.support.EventPayloads
import br.com.itau.challenge.balance.support.KafkaITBase
import br.com.itau.challenge.balance.support.TopicSet
import br.com.itau.challenge.balance.support.backpressureCount
import br.com.itau.challenge.balance.support.backpressureTotal
import br.com.itau.challenge.balance.support.singleValue
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import org.apache.kafka.common.TopicPartition
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Tag("chaos")
class StoreOutageIT : KafkaITBase() {
    override val topics: TopicSet
        get() = topicSet

    @Autowired
    @Qualifier("dynamoDbReadCircuitBreaker")
    private lateinit var circuitBreaker: CircuitBreaker

    @BeforeEach
    fun requireDockerOnCiElseSkip() {
        val available = ComposeControl.isRunning(DYNAMODB_SERVICE)
        val message = "Docker CLI e servico $DYNAMODB_SERVICE do compose deste projeto necessarios"
        if (runningOnCi()) {
            assertTrue(available, "CI: $message (pular em silencio esconderia a perda da cobertura de caos)")
        } else {
            assumeTrue(available, message)
        }
    }

    private fun runningOnCi(): Boolean = !System.getenv("CI").isNullOrBlank()

    // unpause SEMPRE: finally + @AfterEach + @AfterAll + shutdown hook. Nao remover nenhum.
    @AfterEach
    fun alwaysUnpause() {
        ComposeControl.unpauseIgnoringFailure(DYNAMODB_SERVICE)
    }

    private fun backpressureTimeoutPlusUnavailable(): Double = meterRegistry.backpressureCount("timeout") + meterRegistry.backpressureCount("unavailable")

    private fun dependencyUp(): Double = scrape().singleValue("balance_dependency_up", "dependency" to "dynamodb")

    private fun healthStatus(group: String): Int = management("/actuator/health/$group").statusCode()

    private fun awaitDependenciesHealthStatus(
        expected: Int,
        atMost: Duration,
        message: String,
    ) {
        await.atMost(atMost).pollInterval(HEALTH_POLL_INTERVAL).untilAsserted { assertEquals(expected, healthStatus("dependencies"), message) }
    }

    private fun assertHealthDuringOutage(knownAccount: String) {
        awaitDependenciesHealthStatus(503, HEALTH_CONVERGENCE_TIMEOUT, "dependencies")
        assertEquals("""{"status":"DOWN"}""", management("/actuator/health/dependencies").body(), "show-details=never")
        assertEquals(200, healthStatus("readiness"), "a readiness NAO depende do DynamoDB (a instancia continua em rotacao)")
        assertEquals("""{"status":"UP"}""", management("/actuator/health/readiness").body())
        assertEquals(200, healthStatus("liveness"), "a liveness independe do banco")
        assertEquals("""{"status":"UP"}""", management("/actuator/health/liveness").body())
        assertEquals(503, management("/actuator/health").statusCode(), "a raiz agrega as dependencias: nao serve de sonda")
        await.atMost(HEALTH_CONVERGENCE_TIMEOUT).pollInterval(HEALTH_POLL_INTERVAL).untilAsserted { assertEquals(0.0, dependencyUp(), "balance_dependency_up{dependency=dynamodb}") }
        assertEquals(503, get(knownAccount).statusCode(), "a API segue respondendo 503 explicito")
    }

    private fun assertHealthRecoveredAfterUnpause() {
        awaitDependenciesHealthStatus(200, Duration.ofSeconds(20), "dependencies apos o unpause")
        assertEquals("""{"status":"UP"}""", management("/actuator/health/dependencies").body())
        assertEquals(1.0, dependencyUp(), "balance_dependency_up apos o unpause")
        assertEquals(200, healthStatus("readiness"))
        assertEquals(200, healthStatus("liveness"))
        assertEquals(200, management("/actuator/health").statusCode(), "a raiz volta a 200")
    }

    private data class TimedResponse(
        val response: HttpResponse<String>,
        val millis: Long,
    )

    private fun timedGet(accountId: String): TimedResponse {
        val started = System.nanoTime()
        val response = get(accountId)
        return TimedResponse(response, Duration.ofNanos(System.nanoTime() - started).toMillis())
    }

    private fun storedAccountIds(accounts: List<String>): Set<String> = DynamoDbTestSupport.storedBalances(raw, accounts).keys

    private fun awaitAfterUnpause(
        atMost: Duration,
        pollInterval: Duration? = null,
        assertion: () -> Unit,
    ) {
        val awaiting = await.atMost(atMost).ignoreExceptions()
        (pollInterval?.let { awaiting.pollInterval(it) } ?: awaiting).untilAsserted(assertion)
    }

    private fun assertServiceUnavailableAndFast(
        timed: TimedResponse,
        queryNumber: Int,
    ) {
        assertEquals(503, timed.response.statusCode(), "consulta $queryNumber: nunca 404 nem saldo antigo")
        assertEquals("10", timed.response.headers().firstValue("Retry-After").orElse(null), "consulta $queryNumber: Retry-After")
        assertTrue(
            timed.response.headers().firstValue("Content-Type").orElse("").startsWith("application/problem+json"),
            "consulta $queryNumber: Problem Details",
        )
        val body = json.readTree(timed.response.body())
        assertEquals("urn:problem-type:consulta-saldo:servico-indisponivel", body["type"].asString())
        assertTrue(body["balance"] == null && body["owner"] == null, "consulta $queryNumber: sem saldo")
        assertTrue(timed.millis <= MAX_FAST_FAILURE_MS, "consulta $queryNumber: ${timed.millis} ms > $MAX_FAST_FAILURE_MS ms")
    }

    private fun assertSuccessiveQueriesFailFastWith503(account: String) {
        (1..QUERIES_DURING_OUTAGE).map { timedGet(account) }.forEachIndexed { index, timed -> assertServiceUnavailableAndFast(timed, index + 1) }
    }

    private fun burstUntilCircuitOpens(account: String) {
        val pool = Executors.newFixedThreadPool(BURST_THREADS)
        try {
            pool.invokeAll((1..BURST_QUERIES).map { Callable { timedGet(account) } }).forEach { assertEquals(503, it.get().response.statusCode()) }
        } finally {
            pool.shutdownNow()
        }
        assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.state, "circuit breaker abre com a falha sustentada")
    }

    private fun assertOpenCircuitAnswersImmediately(account: String) {
        val fast = timedGet(account)
        assertEquals(503, fast.response.statusCode())
        assertTrue(fast.millis < OPEN_CIRCUIT_MAX_MS, "com o circuito aberto a resposta e imediata: ${fast.millis} ms")
    }

    private fun awaitIngestionRetryingInBackpressure(
        backpressureBefore: Double,
        dltBefore: Map<TopicPartition, Long>,
    ) {
        await.atMost(Duration.ofSeconds(60)).untilAsserted {
            assertTrue(meterRegistry.backpressureTotal() >= backpressureBefore + MIN_BACKPRESSURE_RETRIES_SINGLE_EVENT, "balance.consumer.backpressure deve crescer com a ingestao retentando")
        }
        assertTrue(topics.group.lag() > 0, "o evento fica no broker (lag do grupo > 0)")
        assertEquals(0, topics.dlt.countSince(dltBefore), "DLT inalterado durante a falha")
    }

    private fun awaitStartOfNextBackoffSoUnpauseIsARealResume() {
        val failedBefore = meterRegistry.backpressureTotal()
        await.atMost(Duration.ofSeconds(45)).until { meterRegistry.backpressureTotal() > failedBefore }
    }

    private fun awaitItemWrittenByExpiredWriteAppliedByFrozenDynamoDbLocal(eventAccount: String) {
        awaitAfterUnpause(Duration.ofSeconds(60)) {
            assertEquals(
                setOf(eventAccount),
                storedAccountIds(listOf(eventAccount)),
                "o DynamoDB Local congelado aplica ao voltar a escrita que o SDK deu por expirada: o item pode aparecer antes de o consumer retomar",
            )
        }
    }

    private fun awaitConsumerResumedAtLagZero() {
        topics.group.awaitLagZero(Duration.ofSeconds(60))
    }

    private fun awaitCircuitClosedAfterUnpause(eventAccount: String) {
        awaitAfterUnpause(Duration.ofSeconds(60), Duration.ofMillis(500)) {
            get(eventAccount)
            assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.state)
        }
    }

    @Test
    fun `during the outage the api answers 503 fast, the event stays in the broker outside the dlt and after unpause everything converges`() {
        val known = newAccount()
        publish(EventPayloads.transaction(known, balanceAmount = "10.00"))
        awaitBalance(known, "10.00")
        val dltBefore = topics.dlt.endOffsets()
        val backpressureBefore = meterRegistry.backpressureTotal()
        val unavailableTimeoutBefore = backpressureTimeoutPlusUnavailable()
        val eventAccount = newAccount()

        ComposeControl.pause(DYNAMODB_SERVICE)
        try {
            publish(EventPayloads.transaction(eventAccount, balanceAmount = "321.00"))
            assertSuccessiveQueriesFailFastWith503(known)
            burstUntilCircuitOpens(known)
            assertOpenCircuitAnswersImmediately(known)
            assertHealthDuringOutage(known)
            awaitIngestionRetryingInBackpressure(backpressureBefore, dltBefore)
            awaitStartOfNextBackoffSoUnpauseIsARealResume()
        } finally {
            ComposeControl.unpauseIgnoringFailure(DYNAMODB_SERVICE)
        }

        awaitItemWrittenByExpiredWriteAppliedByFrozenDynamoDbLocal(eventAccount)
        assertEquals(0, topics.dlt.countSince(dltBefore), "DLT segue inalterado")
        awaitConsumerResumedAtLagZero()

        awaitCircuitClosedAfterUnpause(eventAccount)
        awaitBalance(eventAccount, "321.00")
        assertEquals(200, get(known).statusCode())
        assertHealthRecoveredAfterUnpause()
        assertTrue(
            backpressureTimeoutPlusUnavailable() - unavailableTimeoutBefore >= MIN_BACKPRESSURE_RETRIES_SINGLE_EVENT,
            "com o DynamoDB pausado a falha e classificada como timeout/unavailable",
        )
    }

    @Test
    fun `a backlog of 200 events published during the outage is drained after unpause with no loss and no dlt`() {
        val dltBefore = topics.dlt.endOffsets()
        val backpressureBefore = meterRegistry.backpressureTotal()
        val accounts = (1..BACKLOG_EVENTS).map { newAccount() }

        ComposeControl.pause(DYNAMODB_SERVICE)
        try {
            accounts.forEachIndexed { index, account -> publish(EventPayloads.transaction(account, balanceAmount = "${index + 1}.50")) }
            await.atMost(Duration.ofSeconds(60)).untilAsserted {
                assertTrue(meterRegistry.backpressureTotal() >= backpressureBefore + MIN_BACKPRESSURE_RETRIES_BACKLOG, "a ingestao entrou em backpressure")
            }
            assertTrue(topics.group.lag() > 0, "o backlog fica no broker")
            assertEquals(0, topics.dlt.countSince(dltBefore), "nenhum evento valido no DLT durante a falha")
        } finally {
            ComposeControl.unpauseIgnoringFailure(DYNAMODB_SERVICE)
        }

        awaitAfterUnpause(Duration.ofSeconds(90), Duration.ofMillis(500)) { assertEquals(BACKLOG_EVENTS, storedAccountIds(accounts).size, "itens gravados") }
        topics.group.awaitLagZero(Duration.ofSeconds(60))

        assertEquals(accounts.toSet(), storedAccountIds(accounts), "exatamente $BACKLOG_EVENTS itens, 0 perdas")
        assertEquals(0, topics.dlt.countSince(dltBefore), "nenhum evento valido no DLT")
        val balances = DynamoDbTestSupport.storedBalances(raw, accounts)
        accounts.forEachIndexed { index, account ->
            assertEquals(0, "${index + 1}.50".toBigDecimal().compareTo(balances.getValue(account)), "saldo da conta ${index + 1}")
        }
    }

    companion object {
        private const val DYNAMODB_SERVICE = "dynamodb"
        private const val QUERIES_DURING_OUTAGE = 20
        private const val MAX_FAST_FAILURE_MS = 2_000L
        private const val BURST_THREADS = 30
        private const val BURST_QUERIES = 40
        private const val OPEN_CIRCUIT_MAX_MS = 500L
        private const val MIN_BACKPRESSURE_RETRIES_SINGLE_EVENT = 5
        private const val MIN_BACKPRESSURE_RETRIES_BACKLOG = 12
        private const val BACKLOG_EVENTS = 200
        private val HEALTH_CONVERGENCE_TIMEOUT: Duration = Duration.ofSeconds(15)
        private val HEALTH_POLL_INTERVAL: Duration = Duration.ofMillis(500)
        private val topicSet = TopicSet("it-outage")

        @JvmStatic
        @BeforeAll
        fun unpauseIfLeftPausedByPreviousRun() {
            if (ComposeControl.isLeftPaused(DYNAMODB_SERVICE)) ComposeControl.unpauseIgnoringFailure(DYNAMODB_SERVICE)
            Runtime.getRuntime().addShutdownHook(Thread { ComposeControl.unpauseIgnoringFailure(DYNAMODB_SERVICE) })
        }

        @JvmStatic
        @AfterAll
        fun unpauseAtTheEnd() {
            ComposeControl.unpauseIgnoringFailure(DYNAMODB_SERVICE)
        }

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) = topicSet.registerProperties(registry)
    }
}
