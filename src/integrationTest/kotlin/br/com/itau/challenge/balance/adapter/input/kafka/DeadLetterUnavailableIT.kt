package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.support.EventPayloads
import br.com.itau.challenge.balance.support.KafkaITBase
import br.com.itau.challenge.balance.support.TopicSet
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * DLT ausente (`quickstart.md` 6): a mensagem invalida NAO e confirmada (o grupo mantem lag), nada se perde, a falha de
 * publicacao e contada e a mensagem vizinha, na mesma particao e atras da invalida, fica retida. Ao criar o `.DLT` a invalida
 * chega ao DLT com os bytes originais, o lag drena a zero e a vizinha e processada. Contexto e topicos proprios: aqui o topico
 * `.DLT` nao existe (auto-criacao desligada no broker).
 */
class DeadLetterUnavailableIT : KafkaITBase() {
    override val topics: TopicSet
        get() = topicSet

    private fun dltPublishFailures(): Double = meterRegistry.get("balance.dlt.publish.failures").counter().count()

    private fun rejected(reason: String): Double = meterRegistry.get("balance.events").tags("outcome", "rejected", "reason", reason).counter().count()

    @Test
    fun `without the dlt the invalid message is not confirmed and loses nothing, and drains once the dlt exists`() {
        val neighbour = newAccount()
        val invalid = "{\"quebrado\": ".toByteArray(Charsets.UTF_8) + byteArrayOf(0xC3.toByte(), 0x28)
        val failuresBefore = dltPublishFailures()
        val rejectedBefore = rejected("malformed_payload")

        // mesma chave = mesma particao, em ordem: a vizinha valida esta ATRAS da invalida
        topics.publishKeyed("mesma-particao", invalid)
        topics.publishKeyed("mesma-particao", EventPayloads.transaction(neighbour, balanceAmount = "77.00"))

        await.untilAsserted { assertTrue(dltPublishFailures() > failuresBefore, "a falha de publicacao no DLT deve ser contada") }
        val failuresWhileDown = dltPublishFailures()
        assertTrue(topics.groupLag() > 0, "a invalida nao pode ser confirmada com o DLT ausente")
        assertEquals(404, get(neighbour).statusCode(), "a vizinha segue retida atras da invalida (particao bloqueada, sem perda)")
        assertEquals(rejectedBefore, rejected("malformed_payload"), "nada e contado como rejeitado enquanto o DLT nao confirma")
        // o container segue vivo, tentando de novo
        await.untilAsserted { assertTrue(dltPublishFailures() > failuresWhileDown, "o consumo segue tentando") }

        topics.createDlt()

        await.untilAsserted { assertEquals(1, topics.dltCountSince(emptyMap()), "a invalida chega ao DLT") }
        val record = topics.dltRecordsSince(emptyMap()).single()
        assertTrue(record.value().contentEquals(invalid), "bytes originais preservados")
        assertEquals("malformed_payload", record.headers().lastHeader("x-rejection-reason").value().toString(Charsets.UTF_8))
        assertNull(record.headers().lastHeader("x-rejection-detail"))
        topics.awaitLagZero()
        awaitBalance(neighbour, "77.00")
        assertEquals(rejectedBefore + 1, rejected("malformed_payload"), "contada uma unica vez, depois que o DLT confirmou")
    }

    companion object {
        private val topicSet = TopicSet("it-nodlt", createDltOnStart = false)

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) = topicSet.registerProperties(registry)
    }
}
