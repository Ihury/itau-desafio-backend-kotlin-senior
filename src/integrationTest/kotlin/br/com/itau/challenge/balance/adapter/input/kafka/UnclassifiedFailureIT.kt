package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import br.com.itau.challenge.balance.support.EventPayloads
import br.com.itau.challenge.balance.support.KafkaITBase
import br.com.itau.challenge.balance.support.TopicSet
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Falha NAO classificada (defeito nosso): o escritor lanca `IllegalStateException` para uma conta marcada. A mensagem valida tem
 * 3 entregas e vai ao DLT como `unprocessable_event` (para o defeito deterministico nao bloquear a particao para sempre) e a
 * mensagem seguinte, na mesma particao, e processada. Contexto e topicos proprios (`@TestConfiguration`).
 */
class UnclassifiedFailureIT : KafkaITBase() {
    override val topics: TopicSet
        get() = topicSet

    /** Escritor que falha, com defeito interno simulado, para as contas marcadas; as demais seguem para o DynamoDB real. */
    class DefectiveWriter(
        private val delegate: BalanceSnapshotWriter,
    ) : BalanceSnapshotWriter {
        val poisoned: MutableSet<String> = ConcurrentHashMap.newKeySet()
        val attempts = ConcurrentHashMap<String, AtomicInteger>()

        override fun applyIfNewer(snapshot: BalanceSnapshot): ApplyResult {
            val account = snapshot.accountId.value
            if (account !in poisoned) return delegate.applyIfNewer(snapshot)
            attempts.computeIfAbsent(account) { AtomicInteger() }.incrementAndGet()
            throw IllegalStateException("defeito simulado")
        }
    }

    @TestConfiguration
    class Config {
        @Bean
        @Primary
        fun defectiveWriter(
            @Qualifier("balanceSnapshotWriter") delegate: BalanceSnapshotWriter,
        ): DefectiveWriter = DefectiveWriter(delegate)
    }

    @Autowired
    private lateinit var writer: DefectiveWriter

    private fun rejected(reason: String): Double = meterRegistry.get("balance.events").tags("outcome", "rejected", "reason", reason).counter().count()

    @Test
    fun `an unclassified failure gets three deliveries, goes to the dlt as unprocessable event and does not block the partition`() {
        val before = topics.dltEndOffsets()
        val rejectedBefore = rejected("unprocessable_event")
        val poisoned = newAccount()
        val neighbour = newAccount()
        writer.poisoned += poisoned
        val poisonedPayload = EventPayloads.transaction(poisoned, balanceAmount = "13.00")

        // mesma chave = mesma particao: a vizinha esta atras da mensagem com defeito
        topics.publishKeyed("mesma-particao", poisonedPayload)
        topics.publishKeyed("mesma-particao", EventPayloads.transaction(neighbour, balanceAmount = "88.00"))

        awaitBalance(neighbour, "88.00")
        topics.awaitLagZero()
        assertEquals(3, writer.attempts.getValue(poisoned).get(), "exatamente 3 entregas antes do DLT")
        val records = topics.dltRecordsSince(before)
        assertEquals(1, records.size)
        val record = records.single()
        assertEquals(poisonedPayload, record.value().toString(Charsets.UTF_8), "bytes originais preservados")
        assertEquals("unprocessable_event", record.headers().lastHeader("x-rejection-reason").value().toString(Charsets.UTF_8))
        assertNull(record.headers().lastHeader("x-rejection-detail"), "sem caminho de campo para falha interna")
        assertTrue(record.headers().none { it.key().startsWith("kafka_dlt-exception") }, "sem headers de excecao")
        assertEquals(rejectedBefore + 1, rejected("unprocessable_event"))
        assertEquals(404, get(poisoned).statusCode(), "a conta com defeito nunca foi gravada")
    }

    companion object {
        private val topicSet = TopicSet("it-unclassified")

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) = topicSet.registerProperties(registry)
    }
}
