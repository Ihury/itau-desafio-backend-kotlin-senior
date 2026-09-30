package br.com.itau.challenge.balance.support

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
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
 *
 * O conjunto [sharedTopics] e o dos ITs que herdam de [SharedContextKafkaITBase] (UM contexto Spring). Um IT com contexto
 * proprio DEVE usar um [TopicSet] proprio: dois contextos no mesmo grupo dividiriam as particoes.
 */
object IntegrationInfra {
    val bootstrapServers: String = System.getenv("KAFKA_BOOTSTRAP_SERVERS")?.takeIf { it.isNotBlank() } ?: "localhost:19092"

    const val MAIN_PARTITIONS = 12
    const val DLT_PARTITIONS = 3

    init {
        // Timeout padrao de 30 s para toda espera assincrona; os testes com SLO explicito informam o proprio prazo.
        Awaitility.setDefaultTimeout(Duration.ofSeconds(30))
        Awaitility.setDefaultPollInterval(Duration.ofMillis(100))
    }

    val sharedTopics: TopicSet by lazy { TopicSet("it") }

    val topic: String get() = sharedTopics.topic

    val dltTopic: String get() = sharedTopics.dltTopic

    val groupId: String get() = sharedTopics.groupId

    internal fun adminClient(): AdminClient =
        AdminClient.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers, AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG to "15000"))

    internal val producer: KafkaProducer<ByteArray, ByteArray> by lazy {
        KafkaProducer<ByteArray, ByteArray>(
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java.name,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java.name,
                ProducerConfig.ACKS_CONFIG to "all",
            ),
        )
    }

    /** Registra as propriedades do conjunto compartilhado; cria os topicos antes de o contexto subir. */
    fun registerProperties(registry: DynamicPropertyRegistry) = sharedTopics.registerProperties(registry)

    /** Publica os bytes no topico principal compartilhado, sem chave (como o autorizador), e espera a confirmacao do broker. */
    fun publish(payload: ByteArray) = sharedTopics.publish(payload)

    fun publish(payload: String) = sharedTopics.publish(payload)

    fun publishKeyed(
        key: String,
        payload: String,
    ) = sharedTopics.publishKeyed(key, payload)

    fun awaitAssignment(registry: KafkaListenerEndpointRegistry) = sharedTopics.awaitAssignment(registry)
}

/**
 * Topico principal (12 particoes), opcionalmente o `.DLT` (3 particoes, retencao de 14 dias) e o grupo de consumo, todos
 * exclusivos. Com `createDlt = false` o DLT NAO existe (auto-criacao desligada no broker): serve ao teste de DLT ausente, que
 * o cria depois com [createDlt].
 */
class TopicSet(
    prefix: String,
    private val createDltOnStart: Boolean = true,
) {
    private val runId: String = UUID.randomUUID().toString()
    val topic: String = "$prefix-$runId"
    val dltTopic: String = "$topic.DLT"
    val groupId: String = "$prefix-group-$runId"

    private val ensureTopicsCreated: Boolean by lazy {
        IntegrationInfra.adminClient().use { admin ->
            val topics = mutableListOf(NewTopic(topic, IntegrationInfra.MAIN_PARTITIONS, 1.toShort()))
            if (createDltOnStart) topics += dltDefinition()
            admin.createTopics(topics).all().get(30, TimeUnit.SECONDS)
        }
        Runtime.getRuntime().addShutdownHook(
            Thread {
                runCatching { IntegrationInfra.adminClient().use { it.deleteTopics(listOf(topic, dltTopic)).all().get(10, TimeUnit.SECONDS) } }
            },
        )
        true
    }

    private fun dltDefinition(): NewTopic =
        NewTopic(dltTopic, IntegrationInfra.DLT_PARTITIONS, 1.toShort()).configs(mapOf("retention.ms" to "1209600000"))

    /** Cria o `.DLT` depois do inicio (teste de DLT ausente). */
    fun createDlt() {
        IntegrationInfra.adminClient().use { it.createTopics(listOf(dltDefinition())).all().get(30, TimeUnit.SECONDS) }
    }

    /**
     * Propriedades do teste (topicos, grupo, broker, listener ligado e portas aleatorias para a API e o gerenciamento, de modo que
     * um `docker compose up app` na 8080/8082 nunca colida com os ITs); cria os topicos antes de o contexto subir.
     */
    fun properties(): Map<String, String> {
        check(ensureTopicsCreated)
        return mapOf(
            "balance.events.topic" to topic,
            "balance.events.dlt-topic" to dltTopic,
            "spring.kafka.bootstrap-servers" to IntegrationInfra.bootstrapServers,
            "spring.kafka.consumer.group-id" to groupId,
            "spring.kafka.listener.auto-startup" to "true",
            "management.server.port" to "0",
        )
    }

    fun registerProperties(registry: DynamicPropertyRegistry) {
        properties().forEach { (name, value) -> registry.add(name) { value } }
    }

    /** Publica os bytes no topico principal, sem chave (como o autorizador), e espera a confirmacao do broker. */
    fun publish(payload: ByteArray) {
        IntegrationInfra.producer.send(ProducerRecord<ByteArray, ByteArray>(topic, null, payload)).get(15, TimeUnit.SECONDS)
    }

    fun publish(payload: String) = publish(payload.toByteArray(Charsets.UTF_8))

    /**
     * Publica com [key]: registros com a mesma chave caem na MESMA particao e sao consumidos em ordem de publicacao. Serve so
     * aos testes que precisam de uma ordem de chegada deterministica (contagem exata de desfechos, vizinha na mesma particao); o
     * autorizador real publica sem chave e o servico converge em qualquer ordem.
     */
    fun publishKeyed(
        key: String,
        payload: ByteArray,
    ) {
        IntegrationInfra.producer.send(ProducerRecord<ByteArray, ByteArray>(topic, key.toByteArray(Charsets.UTF_8), payload)).get(15, TimeUnit.SECONDS)
    }

    fun publishKeyed(
        key: String,
        payload: String,
    ) = publishKeyed(key, payload.toByteArray(Charsets.UTF_8))

    /**
     * Espera o grupo ESTABILIZAR: todas as particoes atribuidas E cada consumidor do container com ao menos uma. Os consumidores
     * entram em momentos diferentes (num runner lento, segundos depois): o 1o rebalance atribui as 12 particoes a dois deles e
     * o cooperativo seguinte as redistribui. Um rebalance no meio do teste zera a contagem de entregas do `DefaultErrorHandler`
     * (por consumidor) e reentrega o que estava em voo: valido no at-least-once, mas nao e o que estes testes medem.
     */
    fun awaitAssignment(registry: KafkaListenerEndpointRegistry) {
        await.untilAsserted {
            val containers = registry.listenerContainers.filterIsInstance<ConcurrentMessageListenerContainer<*, *>>()
            val assigned =
                containers
                    .flatMap { it.assignedPartitions.orEmpty() }
                    .filter { it.topic() == topic }
                    .toSet()
            if (assigned.size != IntegrationInfra.MAIN_PARTITIONS) {
                throw AssertionError("particoes atribuidas: ${assigned.size} de ${IntegrationInfra.MAIN_PARTITIONS}")
            }
            val consumers = containers.flatMap { it.containers }
            val idle = consumers.count { child -> child.assignedPartitions.orEmpty().none { it.topic() == topic } }
            if (consumers.size <= IntegrationInfra.MAIN_PARTITIONS && idle > 0) {
                throw AssertionError("consumidores ainda sem particao: $idle de ${consumers.size}")
            }
        }
    }

    private fun dltPartitions(): List<TopicPartition> = (0 until IntegrationInfra.DLT_PARTITIONS).map { TopicPartition(dltTopic, it) }

    /** Offset final de cada particao do DLT; marco inicial para [dltCountSince] e [dltRecordsSince]. */
    fun dltEndOffsets(): Map<TopicPartition, Long> =
        IntegrationInfra.adminClient().use { admin ->
            admin
                .listOffsets(dltPartitions().associateWith { OffsetSpec.latest() })
                .all()
                .get(15, TimeUnit.SECONDS)
                .mapValues { it.value.offset() }
        }

    fun dltCountSince(from: Map<TopicPartition, Long>): Int = dltEndOffsets().entries.sumOf { (partition, end) -> end - (from[partition] ?: 0L) }.toInt()

    /** Todas as mensagens do DLT desde [from], lidas sem grupo (atribuicao manual), com os headers. */
    fun dltRecordsSince(from: Map<TopicPartition, Long>): List<ConsumerRecord<ByteArray, ByteArray>> {
        val ends = dltEndOffsets()
        val records = mutableListOf<ConsumerRecord<ByteArray, ByteArray>>()
        KafkaConsumer<ByteArray, ByteArray>(
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to IntegrationInfra.bootstrapServers,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.name,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.name,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
            ),
        ).use { consumer ->
            val partitions = ends.filter { (partition, end) -> end > (from[partition] ?: 0L) }.keys.toList()
            if (partitions.isEmpty()) return emptyList()
            consumer.assign(partitions)
            partitions.forEach { consumer.seek(it, from[it] ?: 0L) }
            val deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos()
            while (partitions.any { consumer.position(it) < ends.getValue(it) } && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach { records += it }
            }
        }
        return records
    }

    /** Mensagens do topico principal ainda nao confirmadas pelo grupo (soma do lag; particao sem commit conta desde o inicio). */
    fun groupLag(): Long =
        IntegrationInfra.adminClient().use { admin ->
            val committed = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get(15, TimeUnit.SECONDS)
            val partitions = (0 until IntegrationInfra.MAIN_PARTITIONS).map { TopicPartition(topic, it) }
            val ends = admin.listOffsets(partitions.associateWith { OffsetSpec.latest() }).all().get(15, TimeUnit.SECONDS)
            partitions.sumOf { partition -> ends.getValue(partition).offset() - (committed[partition]?.offset() ?: 0L) }
        }

    /** Espera o grupo confirmar TODAS as mensagens publicadas (o commit e em lote, depois de processar o poll). */
    fun awaitGroupLagZero(atMost: Duration = Duration.ofSeconds(30)) {
        // `AssertionError` (nao `IllegalStateException`): o Awaitility so repete a condicao quando ela lanca `AssertionError`
        await.atMost(atMost).untilAsserted {
            val lag = groupLag()
            if (lag != 0L) throw AssertionError("lag do grupo: $lag")
        }
    }
}
