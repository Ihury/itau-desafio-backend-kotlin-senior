package br.com.itau.challenge.balance.support

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.awaitility.Awaitility
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.test.context.DynamicPropertyRegistry
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * Apoio dos testes de integracao com o Redpanda do compose (`make integration-test`). A auto-criacao de topicos esta
 * DESLIGADA no broker, entao o topico principal (12 particoes) e o `.DLT` (3) exclusivos do teste, `it-<uuid>`, sao criados
 * aqui via `AdminClient`. O grupo de consumo tambem e exclusivo, para o teste nunca competir com o grupo `consulta-saldo` nem
 * com execucoes anteriores. Os topicos criados sao removidos ao fim da JVM (melhor esforco).
 */
object IntegrationInfra {
    val bootstrapServers: String = System.getenv("KAFKA_BOOTSTRAP_SERVERS")?.takeIf { it.isNotBlank() } ?: "localhost:19092"

    private val runId: String = UUID.randomUUID().toString()

    /** Topico principal exclusivo desta execucao. */
    val topic: String = "it-$runId"

    /** DLT exclusivo desta execucao. */
    val dltTopic: String = "$topic.DLT"

    /** Grupo de consumo exclusivo desta execucao. */
    val groupId: String = "it-group-$runId"

    const val MAIN_PARTITIONS = 12
    const val DLT_PARTITIONS = 3

    init {
        // Timeout padrao de 30 s para toda espera assincrona; os testes com SLO explicito informam o proprio prazo.
        Awaitility.setDefaultTimeout(Duration.ofSeconds(30))
        Awaitility.setDefaultPollInterval(Duration.ofMillis(100))
    }

    private val topicsCreated: Boolean by lazy {
        adminClient().use { admin ->
            admin
                .createTopics(
                    listOf(
                        NewTopic(topic, MAIN_PARTITIONS, 1.toShort()),
                        NewTopic(dltTopic, DLT_PARTITIONS, 1.toShort()).configs(mapOf("retention.ms" to "1209600000")),
                    ),
                ).all()
                .get(30, TimeUnit.SECONDS)
        }
        Runtime.getRuntime().addShutdownHook(
            Thread {
                runCatching { adminClient().use { it.deleteTopics(listOf(topic, dltTopic)).all().get(10, TimeUnit.SECONDS) } }
            },
        )
        true
    }

    private fun adminClient(): AdminClient =
        AdminClient.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers, AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG to "15000"))

    /** Registra as propriedades do teste (`@DynamicPropertySource`); cria os topicos antes de o contexto subir. */
    fun registerProperties(registry: DynamicPropertyRegistry) {
        check(topicsCreated)
        registry.add("balance.events.topic") { topic }
        registry.add("balance.events.dlt-topic") { dltTopic }
        registry.add("spring.kafka.bootstrap-servers") { bootstrapServers }
        registry.add("spring.kafka.consumer.group-id") { groupId }
        registry.add("spring.kafka.listener.auto-startup") { "true" }
    }

    private val producer: KafkaProducer<ByteArray, ByteArray> by lazy {
        KafkaProducer<ByteArray, ByteArray>(
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java.name,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java.name,
                ProducerConfig.ACKS_CONFIG to "all",
            ),
        )
    }

    /** Publica os bytes no topico principal, sem chave (como o autorizador), e espera a confirmacao do broker. */
    fun publish(payload: ByteArray) {
        producer.send(ProducerRecord<ByteArray, ByteArray>(topic, null, payload)).get(15, TimeUnit.SECONDS)
    }

    fun publish(payload: String) = publish(payload.toByteArray(Charsets.UTF_8))

    /**
     * Espera todas as particoes do topico estarem atribuidas aos containers do listener. Sem isso o primeiro teste mediria o
     * tempo de entrada no grupo (rebalance inicial), e nao a latencia de processamento.
     */
    fun awaitAssignment(registry: KafkaListenerEndpointRegistry) {
        await.untilAsserted {
            val assigned =
                registry.listenerContainers
                    .filterIsInstance<ConcurrentMessageListenerContainer<*, *>>()
                    .flatMap { it.assignedPartitions.orEmpty() }
                    .filter { it.topic() == topic }
                    .toSet()
            check(assigned.size == MAIN_PARTITIONS) { "particoes atribuidas: ${assigned.size} de $MAIN_PARTITIONS" }
        }
    }
}
