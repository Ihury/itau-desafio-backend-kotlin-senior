package br.com.itau.challenge.balance.support

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.springframework.test.context.DynamicPropertyRegistry

object IntegrationInfra {
    const val MAIN_PARTITIONS = 12
    const val DLT_PARTITIONS = 3

    private const val ADMIN_REQUEST_TIMEOUT_MS = "15000"

    val bootstrapServers: String = System.getenv("KAFKA_BOOTSTRAP_SERVERS")?.takeIf { it.isNotBlank() } ?: "localhost:19092"

    val sharedTopics: TopicSet by lazy { TopicSet("it") }

    internal fun adminClient(): AdminClient =
        AdminClient.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers, AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG to ADMIN_REQUEST_TIMEOUT_MS))

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

    fun registerSharedProperties(registry: DynamicPropertyRegistry) = sharedTopics.registerProperties(registry)
}
