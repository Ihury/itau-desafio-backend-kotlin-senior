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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConcurrentWritesIT {
    private val writeClient: DynamoDbClient = DynamoDbTestSupport.writeClient()
    private val readerClient: DynamoDbClient = DynamoDbTestSupport.rawClient()
    private val writer = DynamoDbBalanceSnapshotWriter(writeClient, DynamoDbTestSupport.tableName, SimpleMeterRegistry())
    private val reader = DynamoDbBalanceSnapshotReader(readerClient, DynamoDbTestSupport.tableName, true, SimpleMeterRegistry())
    private val created = Collections.synchronizedList(mutableListOf<String>())

    @AfterAll
    fun cleanUp() {
        created.forEach { DynamoDbTestSupport.deleteAccount(readerClient, it) }
        writeClient.close()
        readerClient.close()
    }

    private enum class Ordering(
        val seed: Long,
    ) {
        FULLY_SHUFFLED(1L),
        NEARLY_ASCENDING_WITH_LOCAL_DISORDER_WINDOW_30(2L),
        NEARLY_ASCENDING_WITH_LOCAL_DISORDER_WINDOW_100(3L),
    }

    private class WriteOutcomes {
        val applied = AtomicInteger()
        val obsolete = AtomicInteger()
        val duplicate = AtomicInteger()
        val conflicting = AtomicInteger()
        val failures: MutableList<Throwable> = Collections.synchronizedList(mutableListOf())

        fun record(result: ApplyResult) {
            when (result) {
                ApplyResult.Applied -> applied.incrementAndGet()
                ApplyResult.Obsolete -> obsolete.incrementAndGet()
                is ApplyResult.Duplicate -> {
                    duplicate.incrementAndGet()
                    if (result.conflicting) conflicting.incrementAndGet()
                }
            }
        }

        val total: Int get() = applied.get() + obsolete.get() + duplicate.get()
    }

    private class ConcurrentIntegrityReader(
        private val reader: DynamoDbBalanceSnapshotReader,
        private val accountId: AccountId,
        private val possible: Map<Pair<Long, String>, BalanceSnapshot>,
        private val failures: MutableList<Throwable>,
    ) {
        private val writing = AtomicBoolean(true)
        val reads = AtomicInteger()
        val tornReads: MutableList<String> = Collections.synchronizedList(mutableListOf())
        private val thread = Thread { readUntilWritesFinishAndEnoughSamples() }.also { it.isDaemon = true }

        fun start() = thread.start()

        fun stopAfterWritesFinished() {
            writing.set(false)
            thread.join(JOIN_TIMEOUT_MS)
        }

        private fun readUntilWritesFinishAndEnoughSamples() {
            var last: Pair<Long, String>? = null
            while (writing.get() || (reads.get() < MIN_CONCURRENT_READS && failures.isEmpty())) {
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
        }

        private companion object {
            const val JOIN_TIMEOUT_MS = 30_000L
        }
    }

    private companion object {
        const val THREADS = 32
        const val UNIQUE_KEYS = 300
        const val DUPLICATES = 100
        const val TOTAL = UNIQUE_KEYS + DUPLICATES
        const val MIN_CONCURRENT_READS = 50
        const val WRITES_TIMEOUT_SECONDS = 120L
        const val UPPERCASE_EVERY = 7
        val TX_COUNT = ConvergenceModel.TRANSACTION_IDS_WITH_UUID_COMPARE_TRAPS.size
    }

    private fun nearlyAscendingWithLocalDisorder(
        events: List<EventSpec>,
        random: Random,
        window: Int,
    ): List<EventSpec> =
        events
            .map { it to it.timestampOffsetMicros * TX_COUNT + it.transactionIdIndex + random.nextInt(window) }
            .sortedBy { it.second }
            .map { it.first }

    private fun workload(
        account: String,
        ordering: Ordering,
    ): List<EventSpec> {
        val unique = (0 until UNIQUE_KEYS).map { EventSpec(0, it / TX_COUNT, it % TX_COUNT, uppercaseTransactionId = it % UPPERCASE_EVERY == 0, accountIdOverride = account) }
        val random = Random(ordering.seed)
        val duplicates = (0 until DUPLICATES).map { unique[random.nextInt(unique.size)] }
        val all = unique + duplicates
        return when (ordering) {
            Ordering.FULLY_SHUFFLED -> all.shuffled(random)
            Ordering.NEARLY_ASCENDING_WITH_LOCAL_DISORDER_WINDOW_30 -> nearlyAscendingWithLocalDisorder(all, random, 30)
            Ordering.NEARLY_ASCENDING_WITH_LOCAL_DISORDER_WINDOW_100 -> nearlyAscendingWithLocalDisorder(all, random, 100)
        }
    }

    private fun writeConcurrentlyWithOneBatchPerThread(
        events: List<EventSpec>,
        outcomes: WriteOutcomes,
    ) {
        val start = CountDownLatch(1)
        val done = CountDownLatch(TOTAL)
        val pool = Executors.newFixedThreadPool(THREADS)
        val batches = events.withIndex().groupBy({ it.index % THREADS }, { it.value }).values
        assertEquals(THREADS, batches.size, "uma thread de escrita por lote")
        batches.forEach { batch ->
            pool.submit {
                start.await()
                batch.forEach { spec ->
                    try {
                        outcomes.record(writer.applyIfNewer(spec.toSnapshot()))
                    } catch (failure: Throwable) {
                        outcomes.failures += failure
                    } finally {
                        done.countDown()
                    }
                }
            }
        }
        start.countDown()
        assertTrue(done.await(WRITES_TIMEOUT_SECONDS, TimeUnit.SECONDS), "as $TOTAL escritas terminam em $WRITES_TIMEOUT_SECONDS s")
        pool.shutdown()
    }

    private fun assertClassificationIsDeterministicAfterTheRace(
        events: List<EventSpec>,
        expectedWinner: EventSpec,
    ) {
        assertEquals(ApplyResult.Duplicate(conflicting = false), writer.applyIfNewer(expectedWinner.toSnapshot()))
        val loser = events.first { it.precedenceKey != expectedWinner.precedenceKey }
        assertEquals(ApplyResult.Obsolete, writer.applyIfNewer(loser.toSnapshot()))
    }

    private fun runWith(ordering: Ordering) {
        val account = DynamoDbTestSupport.randomAccountId().also { created += it }
        val accountId = AccountId.parse(account)
        val events = workload(account, ordering)
        assertEquals(TOTAL, events.size)
        val expectedWinner = assertNotNull(ConvergenceModel.winnerOf(events))
        val possible: Map<Pair<Long, String>, BalanceSnapshot> = events.associate { it.precedenceKey to it.toSnapshot() }
        val outcomes = WriteOutcomes()
        val integrityReader = ConcurrentIntegrityReader(reader, accountId, possible, outcomes.failures)

        integrityReader.start()
        writeConcurrentlyWithOneBatchPerThread(events, outcomes)
        integrityReader.stopAfterWritesFinished()

        assertEquals(emptyList(), outcomes.failures, "nenhuma excecao ($ordering)")
        assertEquals(TOTAL, outcomes.total, "cada escrita produz exatamente um desfecho ($ordering)")
        assertEquals(0, outcomes.conflicting.get(), "conteudo derivado da chave nunca diverge")
        assertTrue(outcomes.applied.get() >= 1, "ao menos uma escrita aplicada")
        assertEquals(expectedWinner.toSnapshot(), reader.find(accountId), "snapshot final == max(timestamp, txId) ($ordering)")
        assertClassificationIsDeterministicAfterTheRace(events, expectedWinner)
        assertEquals(emptyList(), integrityReader.tornReads, "leituras concorrentes so veem snapshots integros ($ordering, ${integrityReader.reads.get()} leituras)")
        assertTrue(
            integrityReader.reads.get() >= MIN_CONCURRENT_READS,
            "o leitor concorrente observou ${integrityReader.reads.get()} snapshots (minimo $MIN_CONCURRENT_READS): a verificacao de integridade ficaria vazia",
        )
    }

    @Test
    fun `32 threads writing 400 events of one account in fully shuffled order converge to the highest precedence event`() = runWith(Ordering.FULLY_SHUFFLED)

    @Test
    fun `32 threads writing 400 events of one account in nearly ascending order with local disorder window of 30 converge to the highest precedence event`() =
        runWith(Ordering.NEARLY_ASCENDING_WITH_LOCAL_DISORDER_WINDOW_30)

    @Test
    fun `32 threads writing 400 events of one account in nearly ascending order with local disorder window of 100 converge to the highest precedence event`() =
        runWith(Ordering.NEARLY_ASCENDING_WITH_LOCAL_DISORDER_WINDOW_100)
}
