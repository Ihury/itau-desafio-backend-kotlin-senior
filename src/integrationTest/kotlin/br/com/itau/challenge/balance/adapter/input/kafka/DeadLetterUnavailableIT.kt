package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.support.EventPayloads
import br.com.itau.challenge.balance.support.KafkaITBase
import br.com.itau.challenge.balance.support.TopicSet
import br.com.itau.challenge.balance.support.TopicSet.Companion.SAME_PARTITION_KEY
import br.com.itau.challenge.balance.support.rejectedCount
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DeadLetterUnavailableIT : KafkaITBase() {
    override val topics: TopicSet
        get() = topicSet

    private fun dltPublishFailures(): Double = meterRegistry.get("balance.dlt.publish.failures").counter().count()

    private fun malformedRejections(): Double = meterRegistry.rejectedCount("malformed_payload")

    @Test
    fun `without the dlt the invalid message is not confirmed and loses nothing, and drains once the dlt exists`() {
        val neighbour = newAccount()
        val invalidPayload = "{\"quebrado\": ".toByteArray(Charsets.UTF_8) + byteArrayOf(0xC3.toByte(), 0x28)
        val failuresBefore = dltPublishFailures()
        val rejectedBefore = malformedRejections()

        topics.publishInPartitionOf(SAME_PARTITION_KEY, invalidPayload)
        topics.publishInPartitionOf(SAME_PARTITION_KEY, EventPayloads.transaction(neighbour, balanceAmount = "77.00"))

        await.untilAsserted { assertTrue(dltPublishFailures() > failuresBefore, "a falha de publicacao no DLT deve ser contada") }
        val failuresWhileDown = dltPublishFailures()
        assertTrue(topics.group.lag() > 0, "a invalida nao pode ser confirmada com o DLT ausente")
        assertEquals(404, get(neighbour).statusCode(), "a vizinha segue retida atras da invalida (particao bloqueada, sem perda)")
        assertEquals(rejectedBefore, malformedRejections(), "nada e contado como rejeitado enquanto o DLT nao confirma")
        await.untilAsserted { assertTrue(dltPublishFailures() > failuresWhileDown, "o consumo segue tentando") }

        topics.createDltAfterStart()

        await.untilAsserted { assertEquals(1, topics.dlt.countSince(emptyMap()), "a invalida chega ao DLT") }
        val record = topics.dlt.recordsSince(emptyMap()).single()
        assertTrue(record.value().contentEquals(invalidPayload), "bytes originais preservados")
        assertEquals("malformed_payload", record.headers().lastHeader("x-rejection-reason").value().toString(Charsets.UTF_8))
        assertNull(record.headers().lastHeader("x-rejection-detail"))
        topics.group.awaitLagZero()
        awaitBalance(neighbour, "77.00")
        assertEquals(rejectedBefore + 1, malformedRejections(), "contada uma unica vez, depois que o DLT confirmou")
    }

    companion object {
        private val topicSet = TopicSet("it-nodlt", createDltOnStart = false)

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) = topicSet.registerProperties(registry)
    }
}
