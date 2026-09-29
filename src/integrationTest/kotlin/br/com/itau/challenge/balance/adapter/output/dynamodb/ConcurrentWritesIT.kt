package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.support.DynamoDbTestSupport
import br.com.itau.challenge.balance.testing.ConvergenceModel
import br.com.itau.challenge.balance.testing.ConvergenceModel.EventSpec
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import java.util.Collections
import java.util.Random
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Concorrencia REAL contra o DynamoDB Local (`make integration-test`): 32 threads, liberadas juntas por um
 * `CountDownLatch`, gravam 400 eventos (300 chaves distintas + 100 duplicatas, fora de ordem, com empates de `timestamp`) na
 * MESMA conta pelo `DynamoDbBalanceSnapshotWriter` real, sem lock local. O snapshot final tem de ser o de maior
 * `(timestamp, transactionId)` (FR-007) e cada escrita produz exatamente um desfecho. Um leitor concorrente (leitura
 * fortemente consistente) so pode observar snapshots INTEGROS: todos os campos do MESMO evento, nunca uma mistura (FR-027).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConcurrentWritesIT {
    private val writeClient: DynamoDbClient = DynamoDbTestSupport.writeClient()
    private val readerClient: DynamoDbClient = DynamoDbTestSupport.rawClient()
    private val writer = DynamoDbBalanceSnapshotWriter(writeClient, DynamoDbTestSupport.tableName)
    private val reader = DynamoDbBalanceSnapshotReader(readerClient, DynamoDbTestSupport.tableName, true, SimpleMeterRegistry())
    private val created = Collections.synchronizedList(mutableListOf<String>())

    @AfterAll
    fun cleanUp() {
        created.forEach { readerClient.deleteItem(DeleteItemRequest.builder().tableName(DynamoDbTestSupport.tableName).key(DynamoDbTestSupport.key(it)).build()) }
        writeClient.close()
        readerClient.close()
    }

    private companion object {
        const val THREADS = 32
        const val UNIQUE_KEYS = 300
        const val DUPLICATES = 100
        const val TOTAL = UNIQUE_KEYS + DUPLICATES
        val TX_COUNT = ConvergenceModel.TRANSACTION_IDS.size
    }

    /** 300 chaves distintas (50 timestamps x 6 ids: muitos empates de `timestamp`) + 100 duplicatas, em ordem definida pela semente. */
    private fun workload(
        account: String,
        seed: Long,
    ): List<EventSpec> {
        val unique = (0 until UNIQUE_KEYS).map { EventSpec(0, it / TX_COUNT, it % TX_COUNT, upperCaseTx = it % 7 == 0, accountOverride = account) }
        val random = Random(seed)
        val duplicates = (0 until DUPLICATES).map { unique[random.nextInt(unique.size)] }
        val all = unique + duplicates
        // Semente 1: desordem total. Sementes 2 e 3: quase crescente com desordem local (janelas 30 e 100), que faz varias
        // escritas SEREM aplicadas em disputa (na desordem total o vencedor tende a chegar cedo e quase tudo vira obsoleto).
        return when (seed) {
            1L -> all.shuffled(random)
            else -> {
                val window = if (seed == 2L) 30 else 100
                all.map { it to it.tsOffset * TX_COUNT + it.txIndex + random.nextInt(window) }.sortedBy { it.second }.map { it.first }
            }
        }
    }

    private fun runWith(seed: Long) {
        val account = DynamoDbTestSupport.randomAccountId().also { created += it }
        val accountId = AccountId.parse(account)
        val events = workload(account, seed)
        assertEquals(TOTAL, events.size)
        val expectedWinner = assertNotNull(ConvergenceModel.winnerOf(events))
        val possible: Map<Pair<Long, String>, BalanceSnapshot> = events.associate { it.key to it.toSnapshot() }

        val start = CountDownLatch(1)
        val done = CountDownLatch(TOTAL)
        val applied = AtomicInteger()
        val obsolete = AtomicInteger()
        val duplicate = AtomicInteger()
        val conflicting = AtomicInteger()
        val failures = Collections.synchronizedList(mutableListOf<Throwable>())
        val pool = Executors.newFixedThreadPool(THREADS)

        // Leitor concorrente: cada leitura e um snapshot integro (igual ao derivado de UM evento) e a precedencia nunca regride.
        val writing = AtomicBoolean(true)
        val reads = AtomicInteger()
        val tornReads = Collections.synchronizedList(mutableListOf<String>())
        val readerThread =
            Thread {
                var last: Pair<Long, String>? = null
                while (writing.get()) {
                    try {
                        reader.find(accountId)?.let { seen ->
                            reads.incrementAndGet()
                            val key = seen.precedence.timestamp.micros to seen.precedence.transactionId.value
                            if (possible[key] != seen) tornReads += "leitura mista em $key"
                            val previous = last
                            if (previous != null && (key.first < previous.first || (key.first == previous.first && key.second < previous.second))) {
                                tornReads += "precedencia regrediu de $previous para $key"
                            }
                            last = key
                        }
                    } catch (failure: Throwable) {
                        failures += failure
                    }
                }
            }.also { it.start() }

        events.chunked(TOTAL / THREADS + 1).forEach { batch ->
            pool.submit {
                start.await()
                batch.forEach { spec ->
                    try {
                        when (val result = writer.applyIfNewer(spec.toSnapshot())) {
                            ApplyResult.Applied -> applied.incrementAndGet()
                            ApplyResult.Obsolete -> obsolete.incrementAndGet()
                            is ApplyResult.Duplicate -> {
                                duplicate.incrementAndGet()
                                if (result.conflicting) conflicting.incrementAndGet()
                            }
                        }
                    } catch (failure: Throwable) {
                        failures += failure
                    } finally {
                        done.countDown()
                    }
                }
            }
        }
        start.countDown()
        assertTrue(done.await(120, TimeUnit.SECONDS), "as $TOTAL escritas terminam em 2 minutos")
        writing.set(false)
        readerThread.join(10_000)
        pool.shutdown()

        assertEquals(emptyList(), failures, "nenhuma excecao (seed $seed)")
        assertEquals(TOTAL, applied.get() + obsolete.get() + duplicate.get(), "cada escrita produz exatamente um desfecho (seed $seed)")
        assertEquals(0, conflicting.get(), "conteudo derivado da chave nunca diverge")
        assertTrue(applied.get() >= 1, "ao menos uma escrita aplicada")
        assertEquals(expectedWinner.toSnapshot(), reader.find(accountId), "snapshot final == max(timestamp, txId) (seed $seed)")
        // Depois da corrida a classificacao e deterministica: o vencedor reentregue e duplicado; qualquer outro e obsoleto.
        assertEquals(ApplyResult.Duplicate(conflicting = false), writer.applyIfNewer(expectedWinner.toSnapshot()))
        val loser = events.first { it.key != expectedWinner.key }
        assertEquals(ApplyResult.Obsolete, writer.applyIfNewer(loser.toSnapshot()))
        assertEquals(emptyList(), tornReads, "leituras concorrentes so veem snapshots integros (seed $seed, ${reads.get()} leituras)")
        println("ConcurrentWritesIT seed=$seed threads=$THREADS escritas=$TOTAL applied=${applied.get()} obsolete=${obsolete.get()} duplicate=${duplicate.get()} leituras=${reads.get()} vencedor=${expectedWinner.key}")
    }

    @Test
    fun `32 threads writing 400 events of one account converge to the highest precedence event (seed 1)`() = runWith(1L)

    @Test
    fun `32 threads writing 400 events of one account converge to the highest precedence event (seed 2)`() = runWith(2L)

    @Test
    fun `32 threads writing 400 events of one account converge to the highest precedence event (seed 3)`() = runWith(3L)
}
