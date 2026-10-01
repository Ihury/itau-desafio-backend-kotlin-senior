package br.com.itau.challenge.balance.support

import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import java.time.Duration
import java.util.concurrent.TimeUnit

class DltProbe(
    private val dltTopic: String,
) {
    private fun partitions(): List<TopicPartition> = (0 until IntegrationInfra.DLT_PARTITIONS).map { TopicPartition(dltTopic, it) }

    fun endOffsets(): Map<TopicPartition, Long> =
        IntegrationInfra.adminClient().use { admin ->
            admin
                .listOffsets(partitions().associateWith { OffsetSpec.latest() })
                .all()
                .get(BROKER_CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .mapValues { it.value.offset() }
        }

    fun countSince(from: Map<TopicPartition, Long>): Int = endOffsets().entries.sumOf { (partition, end) -> end - (from[partition] ?: 0L) }.toInt()

    fun recordsSince(from: Map<TopicPartition, Long>): List<ConsumerRecord<ByteArray, ByteArray>> {
        val ends = endOffsets()
        val records = mutableListOf<ConsumerRecord<ByteArray, ByteArray>>()
        KafkaConsumer<ByteArray, ByteArray>(
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to IntegrationInfra.bootstrapServers,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.name,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.name,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
            ),
        ).use { consumer ->
            val partitionsWithRecords = ends.filter { (partition, end) -> end > (from[partition] ?: 0L) }.keys.toList()
            if (partitionsWithRecords.isEmpty()) return emptyList()
            consumer.assign(partitionsWithRecords)
            partitionsWithRecords.forEach { consumer.seek(it, from[it] ?: 0L) }
            val deadline = System.nanoTime() + READ_DEADLINE.toNanos()
            while (partitionsWithRecords.any { consumer.position(it) < ends.getValue(it) } && System.nanoTime() < deadline) {
                consumer.poll(POLL_TIMEOUT).forEach { records += it }
            }
        }
        return records
    }

    private companion object {
        const val BROKER_CALL_TIMEOUT_SECONDS = 15L
        val READ_DEADLINE: Duration = Duration.ofSeconds(15)
        val POLL_TIMEOUT: Duration = Duration.ofMillis(200)
    }
}
