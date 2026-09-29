package br.com.itau.challenge

import br.com.itau.challenge.balance.adapter.output.dynamodb.CircuitBreakingBalanceSnapshotReader
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbClientProperties
import br.com.itau.challenge.balance.port.input.GetBalanceUseCase
import br.com.itau.challenge.balance.port.output.BalanceSnapshotReader
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.test.context.ActiveProfiles
import java.time.Clock
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
}
