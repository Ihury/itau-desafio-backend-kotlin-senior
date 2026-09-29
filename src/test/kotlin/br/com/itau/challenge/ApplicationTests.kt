package br.com.itau.challenge

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.test.context.ActiveProfiles
import kotlin.test.assertTrue

@SpringBootTest
@ActiveProfiles("test")
class ApplicationTests {

	@Autowired
	private lateinit var registry: KafkaListenerEndpointRegistry

	@Test
	fun contextLoads() {
	}

	@Test
	fun `no kafka listener container starts on its own under the test profile`() {
		val running = registry.listenerContainers.filter { it.isRunning }
		assertTrue(running.isEmpty(), "listeners em execucao sem broker: ${running.map { it.listenerId }}")
	}
}
