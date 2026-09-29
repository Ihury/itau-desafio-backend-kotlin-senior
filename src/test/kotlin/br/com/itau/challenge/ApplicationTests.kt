package br.com.itau.challenge

import br.com.itau.challenge.balance.adapter.output.dynamodb.CircuitBreakingBalanceSnapshotReader
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbBalanceSnapshotWriter
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbClientProperties
import br.com.itau.challenge.balance.adapter.output.metrics.MicrometerProcessingMetrics
import br.com.itau.challenge.balance.port.input.GetBalanceUseCase
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import br.com.itau.challenge.balance.port.output.BalanceSnapshotReader
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import br.com.itau.challenge.balance.port.output.ProcessingMetrics
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
import kotlin.test.assertTrue

@SpringBootTest
@ActiveProfiles("test")
class ApplicationTests {

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
	private lateinit var processingMetrics: ProcessingMetrics

	@Autowired
	@Qualifier("dynamoDbReadClient")
	private lateinit var readClient: DynamoDbClient

	@Autowired
	@Qualifier("dynamoDbWriteClient")
	private lateinit var writeClient: DynamoDbClient

	@Test
	fun contextLoads() {
	}

	@Test
	fun `display zone and clock come from the composition root`() {
		assertTrue(displayZone == ZoneId.of("America/Sao_Paulo"))
		assertTrue(clock.zone == java.time.ZoneOffset.UTC)
	}

	@Test
	fun `balance query is wired to the circuit breaking dynamodb reader with the documented defaults`() {
		assertTrue(getBalance.javaClass.simpleName.startsWith("GetBalanceService"))
		assertTrue(reader is CircuitBreakingBalanceSnapshotReader, "leitor inesperado: ${reader.javaClass}")
		assertTrue(dynamoDbProperties.read.consistent)
		assertTrue(dynamoDbProperties.tableName == "AccountBalances")
		assertTrue(dynamoDbProperties.read.maxAttempts == 2)
	}

	@Test
	fun `no kafka listener container starts on its own under the test profile`() {
		val running = registry.listenerContainers.filter { it.isRunning }
		assertTrue(running.isEmpty(), "listeners em execucao sem broker: ${running.map { it.listenerId }}")
	}

	@Test
	fun `ingestion is wired to the conditional dynamodb writer and the micrometer outcome counters`() {
		assertTrue(processEvent.javaClass.simpleName.startsWith("ProcessTransactionEventService"))
		assertTrue(writer is DynamoDbBalanceSnapshotWriter, "escritor inesperado: ${writer.javaClass}")
		assertTrue(processingMetrics is MicrometerProcessingMetrics, "metricas inesperadas: ${processingMetrics.javaClass}")
	}

	@Test
	fun `read and write use distinct dynamodb clients and the write client makes a single attempt`() {
		assertTrue(readClient !== writeClient)
		val read = readClient.serviceClientConfiguration().overrideConfiguration()
		val write = writeClient.serviceClientConfiguration().overrideConfiguration()
		assertTrue(read.retryStrategy().orElseThrow().maxAttempts() == 2)
		assertTrue(write.retryStrategy().orElseThrow().maxAttempts() == 1)
		assertTrue(write.apiCallAttemptTimeout().orElseThrow() == Duration.ofSeconds(2))
		assertTrue(write.apiCallTimeout().orElseThrow() == Duration.ofSeconds(2))
		assertTrue(dynamoDbProperties.write.maxConnections == 50)
	}
}
