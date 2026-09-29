package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbClientProperties
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.CommonErrorHandler
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.test.context.ActiveProfiles
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/** Configuracao efetiva do consumer no contexto Spring completo (perfil test: nenhum container inicia). */
@SpringBootTest
@ActiveProfiles("test")
class KafkaConsumerSettingsTest {
    @Autowired
    private lateinit var consumerFactory: ConsumerFactory<*, *>

    @Autowired
    private lateinit var registry: KafkaListenerEndpointRegistry

    @Autowired
    private lateinit var errorHandler: CommonErrorHandler

    @Autowired
    private lateinit var applicationContext: ApplicationContext

    @Autowired
    private lateinit var dynamoDbProperties: DynamoDbClientProperties

    private val consumerProperties: Map<String, Any> get() = consumerFactory.configurationProperties

    private fun container(): ConcurrentMessageListenerContainer<*, *> {
        val container = registry.getListenerContainer("transaction-event-listener")
        assertNotNull(container, "listener 'transaction-event-listener' nao registrado")
        return container as ConcurrentMessageListenerContainer<*, *>
    }

    @Test
    fun `offsets are committed by the container after persisting and never automatically`() {
        assertEquals("false", consumerProperties["enable.auto.commit"].toString())
        assertEquals(ContainerProperties.AckMode.BATCH, container().containerProperties.ackMode)
    }

    @Test
    fun `keys and values are read as raw bytes that never fail to deserialize`() {
        assertEquals(ByteArrayDeserializer::class.java.name, deserializerName("key.deserializer"))
        assertEquals(ByteArrayDeserializer::class.java.name, deserializerName("value.deserializer"))
    }

    private fun deserializerName(key: String): String =
        when (val value = consumerProperties[key]) {
            is Class<*> -> value.name
            else -> value.toString()
        }

    @Test
    fun `assignment is cooperative and polling is bounded`() {
        assertEquals(CooperativeStickyAssignor::class.java.name, consumerProperties["partition.assignment.strategy"].toString())
        assertEquals(100, consumerProperties["max.poll.records"].toString().toInt())
        assertEquals(300_000, consumerProperties["max.poll.interval.ms"].toString().toInt())
        assertEquals("earliest", consumerProperties["auto.offset.reset"].toString())
        assertEquals("consulta-saldo", consumerProperties["group.id"].toString())
    }

    @Test
    fun `the container uses four consumer threads and stops right after the current record`() {
        assertEquals(4, container().concurrency)
        assertTrue(container().containerProperties.isStopImmediate)
    }

    @Test
    fun `the worst case of a poll fits inside the poll interval`() {
        val maxPollRecords = consumerProperties["max.poll.records"].toString().toLong()
        val maxPollIntervalMs = consumerProperties["max.poll.interval.ms"].toString().toLong()

        assertTrue(
            maxPollRecords * dynamoDbProperties.write.callTimeout.toMillis() < maxPollIntervalMs,
            "max.poll.records x DYNAMODB_WRITE_CALL_TIMEOUT deve ser menor que max.poll.interval.ms",
        )
    }

    @Test
    fun `the container runs the fail safe error handler and never the spring default`() {
        val handler = container().commonErrorHandler

        assertSame(errorHandler, handler, "o container deve usar o CommonErrorHandler do contexto")
        assertTrue(handler is DefaultErrorHandler)
        assertTrue(handler.javaClass == DefaultErrorHandler::class.java)
    }

    @Test
    fun `there is exactly one common error handler in the context`() {
        // Um segundo CommonErrorHandler tornaria a escolha do container imprevisivel.
        assertEquals(listOf("kafkaErrorHandler"), applicationContext.getBeansOfType(CommonErrorHandler::class.java).keys.toList())
    }

    @Test
    fun `no listener container is running under the test profile`() {
        assertTrue(registry.listenerContainers.none { it.isRunning })
    }

    @Test
    fun `topics come from the configuration`() {
        val topics = container().containerProperties.topics
        assertEquals(listOf("transacoes-financeiras-processadas"), topics?.toList())
    }
}
