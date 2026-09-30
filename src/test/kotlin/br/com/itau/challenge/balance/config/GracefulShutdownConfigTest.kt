package br.com.itau.challenge.balance.config

import br.com.itau.challenge.balance.testing.ManagedApplicationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.web.server.Shutdown
import org.springframework.core.env.Environment
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ContainerProperties
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Encerramento gracioso (FR-034, US6.4): a API termina as requisicoes em andamento, o consumer para logo apos o registro corrente
 * e NAO confirma o que nao foi processado (commit so pelo container, depois do listener; sem auto-commit), de modo que o resto e
 * reentregue depois do reinicio e reconciliado pela idempotencia (`duplicate`/`obsolete`).
 */
class GracefulShutdownConfigTest : ManagedApplicationTest() {
    @Autowired
    private lateinit var environment: Environment

    @Autowired
    private lateinit var consumerFactory: ConsumerFactory<*, *>

    @Autowired
    private lateinit var listeners: KafkaListenerEndpointRegistry

    private fun container(): ConcurrentMessageListenerContainer<*, *> =
        assertNotNull(listeners.getListenerContainer("transaction-event-listener")) as ConcurrentMessageListenerContainer<*, *>

    @Test
    fun `the web server shuts down gracefully and each shutdown phase waits at most thirty seconds`() {
        assertEquals("graceful", environment.getProperty("server.shutdown"), "declarado explicitamente, sem depender do padrao do Boot")
        assertEquals("30s", environment.getProperty("spring.lifecycle.timeout-per-shutdown-phase"))
        val bound = Binder.get(environment)
        assertEquals(Shutdown.GRACEFUL, bound.bind("server.shutdown", Shutdown::class.java).get())
        assertEquals(Duration.ofSeconds(30), bound.bind("spring.lifecycle.timeout-per-shutdown-phase", Duration::class.java).get())
    }

    @Test
    fun `the consumer stops right after the current record and only the container commits offsets, after the listener returns`() {
        assertEquals("true", environment.getProperty("spring.kafka.listener.immediate-stop"))
        assertTrue(container().containerProperties.isStopImmediate)
        assertEquals(ContainerProperties.AckMode.BATCH, container().containerProperties.ackMode)
        assertEquals("false", consumerFactory.configurationProperties["enable.auto.commit"].toString(), "o que nao foi persistido nao e confirmado")
    }

    @Test
    fun `the graceful period fits inside the container stop grace of forty seconds`() {
        // docker compose `stop_grace_period: 40s` (Phase 9): o encerramento gracioso precisa caber nele
        val shutdown = Binder.get(environment).bind("spring.lifecycle.timeout-per-shutdown-phase", Duration::class.java).get()
        assertTrue(shutdown < Duration.ofSeconds(40))
    }
}
