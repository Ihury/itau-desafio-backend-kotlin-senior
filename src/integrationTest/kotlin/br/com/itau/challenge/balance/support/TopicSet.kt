package br.com.itau.challenge.balance.support

import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.test.context.DynamicPropertyRegistry
import java.util.UUID
import java.util.concurrent.TimeUnit

class TopicSet(
    prefix: String,
    private val createDltOnStart: Boolean = true,
) {
    private val runId: String = UUID.randomUUID().toString()
    val topic: String = "$prefix-$runId"
    val dltTopic: String = "$topic.DLT"
    val groupId: String = "$prefix-group-$runId"
    val dlt = DltProbe(dltTopic)
    val group = ConsumerGroupProbe(groupId, topic)

    private val ensureTopicsCreated: Boolean by lazy {
        IntegrationInfra.adminClient().use { admin ->
            val topics = mutableListOf(NewTopic(topic, IntegrationInfra.MAIN_PARTITIONS, REPLICATION_FACTOR))
            if (createDltOnStart) topics += dltDefinition()
            admin.createTopics(topics).all().get(TOPIC_CREATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        }
        Runtime.getRuntime().addShutdownHook(
            Thread {
                runCatching { IntegrationInfra.adminClient().use { it.deleteTopics(listOf(topic, dltTopic)).all().get(TOPIC_DELETION_TIMEOUT_SECONDS, TimeUnit.SECONDS) } }
            },
        )
        true
    }

    private fun dltDefinition(): NewTopic =
        NewTopic(dltTopic, IntegrationInfra.DLT_PARTITIONS, REPLICATION_FACTOR).configs(mapOf("retention.ms" to DLT_RETENTION_14_DAYS_MS))

    fun createDltAfterStart() {
        IntegrationInfra.adminClient().use { it.createTopics(listOf(dltDefinition())).all().get(TOPIC_CREATION_TIMEOUT_SECONDS, TimeUnit.SECONDS) }
    }

    fun properties(): Map<String, String> {
        check(ensureTopicsCreated)
        return mapOf(
            "balance.events.topic" to topic,
            "balance.events.dlt-topic" to dltTopic,
            "spring.kafka.bootstrap-servers" to IntegrationInfra.bootstrapServers,
            "spring.kafka.consumer.group-id" to groupId,
            "spring.kafka.listener.auto-startup" to "true",
            "management.server.port" to RANDOM_PORT,
        )
    }

    fun registerProperties(registry: DynamicPropertyRegistry) {
        properties().forEach { (name, value) -> registry.add(name) { value } }
    }

    fun publish(payload: ByteArray) {
        IntegrationInfra.producer.send(ProducerRecord<ByteArray, ByteArray>(topic, null, payload)).get(PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    }

    fun publish(payload: String) = publish(payload.toByteArray(Charsets.UTF_8))

    fun publishInPartitionOf(
        key: String,
        payload: ByteArray,
    ) {
        IntegrationInfra.producer.send(ProducerRecord<ByteArray, ByteArray>(topic, key.toByteArray(Charsets.UTF_8), payload)).get(PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    }

    fun publishInPartitionOf(
        key: String,
        payload: String,
    ) = publishInPartitionOf(key, payload.toByteArray(Charsets.UTF_8))

    companion object {
        const val SAME_PARTITION_KEY = "mesma-particao"

        private const val REPLICATION_FACTOR: Short = 1
        private const val DLT_RETENTION_14_DAYS_MS = "1209600000"
        private const val RANDOM_PORT = "0"
        private const val TOPIC_CREATION_TIMEOUT_SECONDS = 30L
        private const val TOPIC_DELETION_TIMEOUT_SECONDS = 10L
        private const val PUBLISH_TIMEOUT_SECONDS = 15L
    }
}
