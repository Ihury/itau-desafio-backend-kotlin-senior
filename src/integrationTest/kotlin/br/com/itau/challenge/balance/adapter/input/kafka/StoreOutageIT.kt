package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.support.ComposeControl
import br.com.itau.challenge.balance.support.DynamoDbTestSupport
import br.com.itau.challenge.balance.support.EventPayloads
import br.com.itau.challenge.balance.support.KafkaITBase
import br.com.itau.challenge.balance.support.TopicSet
import br.com.itau.challenge.balance.support.single
import io.github.resilience4j.circuitbreaker.CircuitBreaker
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
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes
import java.math.BigDecimal
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Indisponibilidade REAL do armazenamento (`docker compose pause dynamodb`; `quickstart.md` secao 8): a API falha rapido com 503 e
 * `Retry-After`, a ingestao segura as mensagens no broker (nunca DLT, nunca perda) e, apos o `unpause`, o backlog drena sozinho e
 * o circuito fecha (SC-007, SC-008, FR-025, FR-028). O `unpause` SEMPRE roda (`finally`, `@AfterEach` e `@AfterAll`). Pulado
 * quando o Docker CLI ou o servico `dynamodb` deste projeto compose nao estao disponiveis. Contexto e topicos proprios.
 */
@Tag("chaos")
class StoreOutageIT : KafkaITBase() {
    override val topics: TopicSet
        get() = topicSet

    @Autowired
    @Qualifier("dynamoDbReadCircuitBreaker")
    private lateinit var circuitBreaker: CircuitBreaker

    @BeforeEach
    fun requireDockerAndDynamoDb() {
        val available = ComposeControl.isRunning(SERVICE)
        val message = "Docker CLI e servico $SERVICE do compose deste projeto necessarios"
        // No CI (variavel `CI` definida) a indisponibilidade do Docker/compose FALHA o teste: pular em silencio esconderia a perda
        // da cobertura de caos. Localmente continua pulando para nao exigir Docker de quem so roda os ITs sem o teste de caos.
        if (runningOnCi()) assertTrue(available, "CI: $message") else assumeTrue(available, message)
    }

    private fun runningOnCi(): Boolean = !System.getenv("CI").isNullOrBlank()

    @AfterEach
    fun alwaysUnpause() {
        ComposeControl.unpause(SERVICE)
    }

    private fun backpressureTotal(): Double = meterRegistry.find("balance.consumer.backpressure").counters().sumOf { it.count() }

    private fun backpressure(cause: String): Double = meterRegistry.get("balance.consumer.backpressure").tag("cause", cause).counter().count()

    private fun dependencyUp(): Double = scrape().single("balance_dependency_up", "dependency" to "dynamodb")

    private fun status(group: String): Int = management("/actuator/health/$group").statusCode()

    /** Saude com o DynamoDB fora (FR-033): so `dependencies` cai; a instancia continua em rotacao (liveness e readiness 200). */
    private fun assertHealthDuringOutage(knownAccount: String) {
        // o probe tem timeout curto e o resultado fica em cache por 5 s: dentro de 5 s + cache o grupo cai
        await.atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(500)).untilAsserted { assertEquals(503, status("dependencies"), "dependencies") }
        assertEquals("""{"status":"DOWN"}""", management("/actuator/health/dependencies").body(), "show-details=never")
        assertEquals(200, status("readiness"), "a readiness NAO depende do DynamoDB (a instancia continua em rotacao)")
        assertEquals("""{"status":"UP"}""", management("/actuator/health/readiness").body())
        assertEquals(200, status("liveness"), "a liveness independe do banco")
        assertEquals("""{"status":"UP"}""", management("/actuator/health/liveness").body())
        assertEquals(503, management("/actuator/health").statusCode(), "a raiz agrega as dependencias: nao serve de sonda")
        await.atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(500)).untilAsserted { assertEquals(0.0, dependencyUp(), "balance_dependency_up{dependency=dynamodb}") }
        assertEquals(503, get(knownAccount).statusCode(), "a API segue respondendo 503 explicito")
    }

    private data class Timed(
        val response: HttpResponse<String>,
        val millis: Long,
    )

    private fun timedGet(accountId: String): Timed {
        val started = System.nanoTime()
        val response = get(accountId)
        return Timed(response, Duration.ofNanos(System.nanoTime() - started).toMillis())
    }

    private fun items(accounts: List<String>): Set<String> =
        accounts
            .chunked(100)
            .flatMap { chunk ->
                val keys = chunk.map { DynamoDbTestSupport.key(it) }
                val request = BatchGetItemRequest.builder().requestItems(mapOf(DynamoDbTestSupport.tableName to KeysAndAttributes.builder().keys(keys).consistentRead(true).build())).build()
                raw.batchGetItem(request).responses()[DynamoDbTestSupport.tableName].orEmpty().map { it["pk"]!!.s().removePrefix("ACCOUNT#") }
            }.toSet()

    @Test
    fun `during the outage the api answers 503 fast, the event stays in the broker outside the dlt and after unpause everything converges`() {
        val known = newAccount()
        publish(EventPayloads.transaction(known, balanceAmount = "10.00"))
        awaitBalance(known, "10.00")
        val dltBefore = topics.dltEndOffsets()
        val backpressureBefore = backpressureTotal()
        val unavailableTimeoutBefore = backpressure("timeout") + backpressure("unavailable")
        val eventAccount = newAccount()

        ComposeControl.pause(SERVICE)
        try {
            publish(EventPayloads.transaction(eventAccount, balanceAmount = "321.00"))
            // 20 consultas sucessivas: cada uma 503 + Retry-After 10, em <= 2 s, nunca 404 nem saldo antigo (SC-008)
            val queries = (1..20).map { timedGet(known) }
            queries.forEachIndexed { index, timed ->
                assertEquals(503, timed.response.statusCode(), "consulta ${index + 1}: nunca 404 nem saldo antigo")
                assertEquals("10", timed.response.headers().firstValue("Retry-After").orElse(null), "consulta ${index + 1}: Retry-After")
                assertTrue(
                    timed.response.headers().firstValue("Content-Type").orElse("").startsWith("application/problem+json"),
                    "consulta ${index + 1}: Problem Details",
                )
                val body = json.readTree(timed.response.body())
                assertEquals("urn:problem-type:consulta-saldo:servico-indisponivel", body["type"].asString())
                assertTrue(body["balance"] == null && body["owner"] == null, "consulta ${index + 1}: sem saldo")
                assertTrue(timed.millis <= 2_000, "consulta ${index + 1}: ${timed.millis} ms > 2 s (SC-008)")
            }
            println("OUTAGE-503-MS=${queries.map { it.millis }}")

            // rajada concorrente: as chamadas lentas somam o minimo de chamadas da janela e o circuito abre (falha rapida)
            val pool = Executors.newFixedThreadPool(30)
            try {
                pool.invokeAll((1..40).map { Callable { timedGet(known) } }).forEach { assertEquals(503, it.get().response.statusCode()) }
            } finally {
                pool.shutdownNow()
            }
            assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.state, "circuit breaker abre com a falha sustentada")
            val fast = timedGet(known)
            assertEquals(503, fast.response.statusCode())
            assertTrue(fast.millis < 500, "com o circuito aberto a resposta e imediata: ${fast.millis} ms")
            println("OUTAGE-503-OPEN-CIRCUIT-MS=${fast.millis}")
            assertHealthDuringOutage(known)

            // ingestao durante a falha (o evento foi publicado logo apos o pause, junto das consultas): ele fica no broker, nada vai
            // ao DLT e o backpressure cresce a cada tentativa, com o backoff (500 ms x2, jitter) chegando a varios segundos
            await.atMost(Duration.ofSeconds(60)).untilAsserted {
                assertTrue(backpressureTotal() >= backpressureBefore + 5, "balance.consumer.backpressure deve crescer com a ingestao retentando")
            }
            assertTrue(topics.groupLag() > 0, "o evento fica no broker (lag do grupo > 0)")
            assertEquals(0, topics.dltCountSince(dltBefore), "DLT inalterado durante a falha")
            // desfaz a pausa logo depois de uma nova falha: o container esta no INICIO de uma espera longa do backoff, entao o tempo de
            // drenagem medido abaixo e o de uma retomada real (e nao o de uma tentativa que por acaso estava em voo)
            val failedBefore = backpressureTotal()
            await.atMost(Duration.ofSeconds(45)).until { backpressureTotal() > failedBefore }
            println("OUTAGE-FAILED-DELIVERIES-AT-UNPAUSE=${backpressureTotal() - backpressureBefore}")
        } finally {
            ComposeControl.unpause(SERVICE)
        }
        val unpausedAt = System.nanoTime()

        // retomada sem intervencao: em <= 60 s o saldo reflete o evento (lido direto do DynamoDB, independente do circuito)
        // `ignoreExceptions`: logo apos o `unpause` o SDK ainda pode lancar (timeout/conexao) ate o DynamoDB Local responder de novo
        await.atMost(Duration.ofSeconds(60)).ignoreExceptions().untilAsserted { assertEquals(setOf(eventAccount), items(listOf(eventAccount))) }
        val drainMillis = Duration.ofNanos(System.nanoTime() - unpausedAt).toMillis()
        println("OUTAGE-DRAIN-MS=$drainMillis")
        assertEquals(0, topics.dltCountSince(dltBefore), "DLT segue inalterado")
        // O item pode aparecer logo apos o unpause: a escrita que o SDK deu por expirada ficou na fila do socket do DynamoDB Local
        // congelado e ele a aplica ao voltar (a escrita condicional e idempotente; a nova entrega sera um `duplicate`). A retomada
        // do consumer, essa sim, so termina quando a espera do backoff acaba e o offset e confirmado: e o que se mede aqui.
        topics.awaitLagZero(Duration.ofSeconds(60))
        println("OUTAGE-LAG-ZERO-MS=${Duration.ofNanos(System.nanoTime() - unpausedAt).toMillis()}")

        // o circuito fecha sozinho (OPEN -> HALF_OPEN -> CLOSED) e a API volta a responder o saldo novo
        await.atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500)).ignoreExceptions().untilAsserted {
            get(eventAccount)
            assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.state)
        }
        awaitBalance(eventAccount, "321.00")
        assertEquals(200, get(known).statusCode())
        // a dependencia volta sozinha (cache de 5 s): dependencies 200, gauge 1; liveness e readiness nunca cairam
        await.atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(500)).untilAsserted { assertEquals(200, status("dependencies"), "dependencies apos o unpause") }
        assertEquals("""{"status":"UP"}""", management("/actuator/health/dependencies").body())
        assertEquals(1.0, dependencyUp(), "balance_dependency_up apos o unpause")
        assertEquals(200, status("readiness"))
        assertEquals(200, status("liveness"))
        assertEquals(200, management("/actuator/health").statusCode(), "a raiz volta a 200")
        assertTrue(
            backpressure("timeout") + backpressure("unavailable") - unavailableTimeoutBefore >= 5,
            "com o DynamoDB pausado a falha e classificada como timeout/unavailable",
        )
        println(
            "OUTAGE-BACKPRESSURE throttled=${backpressure("throttled")} unavailable=${backpressure("unavailable")} timeout=${backpressure("timeout")}",
        )
    }

    @Test
    fun `a backlog of 200 events published during the outage is drained after unpause with no loss and no dlt`() {
        val dltBefore = topics.dltEndOffsets()
        val backpressureBefore = backpressureTotal()
        val accounts = (1..200).map { newAccount() }

        ComposeControl.pause(SERVICE)
        try {
            accounts.forEachIndexed { index, account -> publish(EventPayloads.transaction(account, balanceAmount = "${index + 1}.50")) }
            // segura a falha ate as threads de consumo retentarem varias vezes (o backoff cresce e as esperas ficam longas)
            await.atMost(Duration.ofSeconds(60)).untilAsserted { assertTrue(backpressureTotal() >= backpressureBefore + 12, "a ingestao entrou em backpressure") }
            assertTrue(topics.groupLag() > 0, "o backlog fica no broker")
            assertEquals(0, topics.dltCountSince(dltBefore), "nenhum evento valido no DLT durante a falha")
        } finally {
            ComposeControl.unpause(SERVICE)
        }
        val unpausedAt = System.nanoTime()

        await.atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofMillis(500)).ignoreExceptions().untilAsserted { assertEquals(200, items(accounts).size, "itens gravados") }
        println("BACKLOG-DRAIN-MS=${Duration.ofNanos(System.nanoTime() - unpausedAt).toMillis()}")
        topics.awaitLagZero(Duration.ofSeconds(60))
        println("BACKLOG-LAG-ZERO-MS=${Duration.ofNanos(System.nanoTime() - unpausedAt).toMillis()}")

        assertEquals(accounts.toSet(), items(accounts), "exatamente 200 itens, 0 perdas (SC-007)")
        assertEquals(0, topics.dltCountSince(dltBefore), "nenhum evento valido no DLT")
        // cada saldo e o do seu evento (nada trocado nem perdido)
        accounts.forEachIndexed { index, account ->
            val item = raw.getItem { it.tableName(DynamoDbTestSupport.tableName).key(DynamoDbTestSupport.key(account)).consistentRead(true) }.item()
            assertEquals(0, BigDecimal("${index + 1}.50").compareTo(BigDecimal(item.getValue("balanceAmount").n())), "saldo da conta ${index + 1}")
        }
    }

    companion object {
        private const val SERVICE = "dynamodb"
        private val topicSet = TopicSet("it-outage")

        @JvmStatic
        @BeforeAll
        fun unpauseLeftovers() {
            // um `pause` esquecido por uma execucao anterior interrompida quebraria todos os ITs seguintes
            if (ComposeControl.isPaused(SERVICE)) ComposeControl.unpause(SERVICE)
            Runtime.getRuntime().addShutdownHook(Thread { ComposeControl.unpause(SERVICE) })
        }

        @JvmStatic
        @AfterAll
        fun unpauseAtTheEnd() {
            ComposeControl.unpause(SERVICE)
        }

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) = topicSet.registerProperties(registry)
    }
}
