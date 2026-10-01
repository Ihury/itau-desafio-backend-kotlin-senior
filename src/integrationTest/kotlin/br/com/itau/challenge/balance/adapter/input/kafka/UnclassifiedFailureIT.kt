package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.support.DefectiveWriter
import br.com.itau.challenge.balance.support.DefectiveWriterConfig
import br.com.itau.challenge.balance.support.EventPayloads
import br.com.itau.challenge.balance.support.KafkaITBase
import br.com.itau.challenge.balance.support.TopicSet
import br.com.itau.challenge.balance.support.TopicSet.Companion.SAME_PARTITION_KEY
import br.com.itau.challenge.balance.support.rejectedCount
import org.awaitility.kotlin.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Import
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@Import(DefectiveWriterConfig::class)
class UnclassifiedFailureIT : KafkaITBase() {
    override val topics: TopicSet
        get() = topicSet

    @Autowired
    private lateinit var writer: DefectiveWriter

    private fun unprocessableRejections(): Double = meterRegistry.rejectedCount("unprocessable_event")

    private fun awaitFirstFailedDelivery(account: String) {
        await.pollInterval(FIRST_DELIVERY_POLL_INTERVAL).atMost(FIRST_DELIVERY_TIMEOUT).until { (writer.attempts[account]?.get() ?: 0) >= 1 }
    }

    private fun ownerContainerOf(consumerThread: String): MessageListenerContainer =
        assertNotNull(
            registry.listenerContainers
                .filterIsInstance<ConcurrentMessageListenerContainer<*, *>>()
                .flatMap { it.containers }
                .firstOrNull { consumerThread.startsWith("${it.beanName}-C-") },
            "container filho dono de $consumerThread",
        )

    @Test
    fun `an unclassified failure gets three deliveries, goes to the dlt as unprocessable event and does not block the partition`() {
        val before = topics.dlt.endOffsets()
        val rejectedBefore = unprocessableRejections()
        val poisoned = newAccount()
        val neighbour = newAccount()
        writer.poisoned += poisoned
        val poisonedPayload = EventPayloads.transaction(poisoned, balanceAmount = "13.00")

        topics.publishInPartitionOf(SAME_PARTITION_KEY, poisonedPayload)
        topics.publishInPartitionOf(SAME_PARTITION_KEY, EventPayloads.transaction(neighbour, balanceAmount = "88.00"))

        awaitBalance(neighbour, "88.00")
        topics.group.awaitLagZero()
        assertEquals(3, writer.attempts.getValue(poisoned).get(), "exatamente 3 entregas antes do DLT")
        val records = topics.dlt.recordsSince(before)
        assertEquals(1, records.size)
        val record = records.single()
        assertEquals(poisonedPayload, record.value().toString(Charsets.UTF_8), "bytes originais preservados")
        assertEquals("unprocessable_event", record.headers().lastHeader("x-rejection-reason").value().toString(Charsets.UTF_8))
        assertNull(record.headers().lastHeader("x-rejection-detail"), "sem caminho de campo para falha interna")
        assertTrue(record.headers().none { it.key().startsWith("kafka_dlt-exception") }, "sem headers de excecao")
        assertEquals(rejectedBefore + 1, unprocessableRejections())
        assertEquals(404, get(poisoned).statusCode(), "a conta com defeito nunca foi gravada")
    }

    @Test
    fun `a consumer leaving the group in the middle of the deliveries restarts the count but the dlt still gets the message exactly once`() {
        val before = topics.dlt.endOffsets()
        val rejectedBefore = unprocessableRejections()
        val poisoned = newAccount()
        val neighbour = newAccount()
        writer.poisoned += poisoned
        val poisonedPayload = EventPayloads.transaction(poisoned, balanceAmount = "14.00")

        topics.publishInPartitionOf(SAME_PARTITION_KEY, poisonedPayload)
        topics.publishInPartitionOf(SAME_PARTITION_KEY, EventPayloads.transaction(neighbour, balanceAmount = "77.00"))

        awaitFirstFailedDelivery(poisoned)
        val owner = ownerContainerOf(assertNotNull(writer.lastThread[poisoned]))
        val attemptsBeforeLeaving = writer.attempts.getValue(poisoned).get()
        owner.stop()
        try {
            awaitBalance(neighbour, "77.00")
            topics.group.awaitLagZero()
        } finally {
            owner.start()
        }
        val deliveries = writer.attempts.getValue(poisoned).get()
        assertTrue(deliveries >= 3, "no minimo 3 entregas antes do DLT: $deliveries")
        assertTrue(
            deliveries > 3,
            "o dono saiu depois de $attemptsBeforeLeaving entrega(s) e o novo dono recomecou a contagem, logo passa de 3: $deliveries",
        )
        assertEquals(1, topics.dlt.recordsSince(before).size, "o DLT recebe a mensagem exatamente uma vez")
        assertEquals(rejectedBefore + 1, unprocessableRejections(), "o desfecho e contado exatamente uma vez")
        assertEquals(404, get(poisoned).statusCode(), "a conta com defeito nunca foi gravada")
    }

    companion object {
        private val FIRST_DELIVERY_POLL_INTERVAL: Duration = Duration.ofMillis(2)
        private val FIRST_DELIVERY_TIMEOUT: Duration = Duration.ofSeconds(10)
        private val topicSet = TopicSet("it-unclassified")

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) = topicSet.registerProperties(registry)
    }
}
