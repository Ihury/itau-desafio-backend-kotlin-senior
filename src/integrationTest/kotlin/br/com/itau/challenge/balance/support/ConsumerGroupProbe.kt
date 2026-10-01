package br.com.itau.challenge.balance.support

import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.common.TopicPartition
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals

class ConsumerGroupProbe(
    private val groupId: String,
    private val topic: String,
) {
    fun lag(): Long =
        IntegrationInfra.adminClient().use { admin ->
            val committed = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get(BROKER_CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            val partitions = (0 until IntegrationInfra.MAIN_PARTITIONS).map { TopicPartition(topic, it) }
            val ends = admin.listOffsets(partitions.associateWith { OffsetSpec.latest() }).all().get(BROKER_CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            partitions.sumOf { partition -> ends.getValue(partition).offset() - (committed[partition]?.offset() ?: 0L) }
        }

    fun awaitLagZero(atMost: Duration = DEFAULT_LAG_ZERO_TIMEOUT) {
        await.atMost(atMost).untilAsserted { assertEquals(0L, lag(), "lag do grupo $groupId") }
    }

    fun awaitStabilized(registry: KafkaListenerEndpointRegistry) {
        await.untilAsserted {
            val containers = registry.listenerContainers.filterIsInstance<ConcurrentMessageListenerContainer<*, *>>()
            val assigned =
                containers
                    .flatMap { it.assignedPartitions.orEmpty() }
                    .filter { it.topic() == topic }
                    .toSet()
            assertEquals(IntegrationInfra.MAIN_PARTITIONS, assigned.size, REBALANCE_MESSAGE)
            val consumers = containers.flatMap { it.containers }
            val idle = consumers.count { child -> child.assignedPartitions.orEmpty().none { it.topic() == topic } }
            if (consumers.size <= IntegrationInfra.MAIN_PARTITIONS) {
                assertEquals(0, idle, "consumidores ainda sem particao ($REBALANCE_MESSAGE)")
            }
        }
    }

    private companion object {
        const val BROKER_CALL_TIMEOUT_SECONDS = 15L
        val DEFAULT_LAG_ZERO_TIMEOUT: Duration = Duration.ofSeconds(30)
        const val REBALANCE_MESSAGE =
            "particoes atribuidas; um rebalance no meio do teste zera a contagem de entregas do DefaultErrorHandler e reentrega o que estava em voo"
    }
}
