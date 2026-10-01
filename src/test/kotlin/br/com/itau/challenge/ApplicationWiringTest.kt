package br.com.itau.challenge

import br.com.itau.challenge.balance.adapter.input.kafka.TransactionEventParser
import br.com.itau.challenge.balance.adapter.output.dynamodb.CircuitBreakingBalanceSnapshotReader
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbBalanceSnapshotWriter
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbClientProperties
import br.com.itau.challenge.balance.adapter.output.metrics.MicrometerProcessingMetrics
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.port.input.GetBalanceUseCase
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import br.com.itau.challenge.balance.port.output.BalanceSnapshotReader
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import br.com.itau.challenge.balance.port.output.OutcomeMetrics
import br.com.itau.challenge.balance.testing.DISPLAY_ZONE
import br.com.itau.challenge.balance.testing.TransactionPayloads
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.test.context.ActiveProfiles
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.time.Clock
import java.time.Duration
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotSame
import kotlin.test.assertTrue

@SpringBootTest
@ActiveProfiles("test")
class ApplicationWiringTest {
    @Autowired
    private lateinit var registry: KafkaListenerEndpointRegistry

    @Autowired
    private lateinit var getBalance: GetBalanceUseCase

    @Autowired
    private lateinit var reader: BalanceSnapshotReader

    @Autowired
    private lateinit var dynamoDbProperties: DynamoDbClientProperties

    @Autowired
    private lateinit var displayZone: ZoneId

    @Autowired
    private lateinit var clock: Clock

    @Autowired
    private lateinit var processEvent: ProcessTransactionEventUseCase

    @Autowired
    private lateinit var writer: BalanceSnapshotWriter

    @Autowired
    private lateinit var outcomeMetrics: OutcomeMetrics

    @Autowired
    @Qualifier("dynamoDbReadClient")
    private lateinit var readClient: DynamoDbClient

    @Autowired
    @Qualifier("dynamoDbWriteClient")
    private lateinit var writeClient: DynamoDbClient

    @Autowired
    private lateinit var parser: TransactionEventParser

    @Test
    fun `the application context loads`() {
    }

    @Test
    fun `display zone and clock come from the composition root`() {
        assertEquals(DISPLAY_ZONE, displayZone)
        assertEquals(ZoneOffset.UTC, clock.zone)
    }

    @Test
    fun `balance query is wired to the circuit breaking dynamodb reader with the documented defaults`() {
        assertTrue(getBalance.javaClass.simpleName.startsWith("GetBalanceService"))
        assertIs<CircuitBreakingBalanceSnapshotReader>(reader, "leitor inesperado: ${reader.javaClass}")
        assertTrue(dynamoDbProperties.read.consistent)
        assertEquals("AccountBalances", dynamoDbProperties.tableName)
        assertEquals(2, dynamoDbProperties.read.maxAttempts)
    }

    @Test
    fun `no kafka listener container starts on its own under the test profile`() {
        val running = registry.listenerContainers.filter { it.isRunning }
        assertTrue(running.isEmpty(), "listeners em execucao sem broker: ${running.map { it.listenerId }}")
    }

    @Test
    fun `ingestion is wired to the conditional dynamodb writer and the micrometer outcome counters`() {
        assertTrue(processEvent.javaClass.simpleName.startsWith("ProcessTransactionEventService"))
        assertIs<DynamoDbBalanceSnapshotWriter>(writer, "escritor inesperado: ${writer.javaClass}")
        assertIs<MicrometerProcessingMetrics>(outcomeMetrics, "metricas inesperadas: ${outcomeMetrics.javaClass}")
    }

    @Test
    fun `read and write use distinct dynamodb clients`() {
        assertNotSame(readClient, writeClient)
    }

    @Test
    fun `the read client makes two attempts`() {
        val read = readClient.serviceClientConfiguration().overrideConfiguration()

        assertEquals(2, read.retryStrategy().orElseThrow().maxAttempts())
    }

    @Test
    fun `the write client makes a single attempt with two second timeouts and its own pool size`() {
        val write = writeClient.serviceClientConfiguration().overrideConfiguration()

        assertEquals(1, write.retryStrategy().orElseThrow().maxAttempts())
        assertEquals(Duration.ofSeconds(2), write.apiCallAttemptTimeout().orElseThrow())
        assertEquals(Duration.ofSeconds(2), write.apiCallTimeout().orElseThrow())
        assertEquals(50, dynamoDbProperties.write.maxConnections)
    }

    private fun payloadWithTimestamps(
        transactionTimestamp: String,
        accountCreatedAt: String,
    ): ByteArray =
        TransactionPayloads
            .json(
                mapOf(
                    "transaction.amount" to "1",
                    "transaction.timestamp" to transactionTimestamp,
                    "account.created_at" to accountCreatedAt,
                    "account.balance.amount" to "1",
                ),
            ).toByteArray()

    @Test
    fun `the event parser accepts exactly 2000-01-01 for transactions and 1900-01-01 for account creation`() {
        parser.parse(payloadWithTimestamps("946684800000000", "-2208988800000000"))
    }

    @Test
    fun `the event parser rejects one microsecond before each documented minimum timestamp`() {
        listOf(payloadWithTimestamps("946684799999999", "0"), payloadWithTimestamps("946684800000000", "-2208988800000001")).forEach { bad ->
            val failure = runCatching { parser.parse(bad) }.exceptionOrNull()
            assertTrue(failure is InvalidEventException, "minimo nao aplicado: ${String(bad)}")
        }
    }
}
