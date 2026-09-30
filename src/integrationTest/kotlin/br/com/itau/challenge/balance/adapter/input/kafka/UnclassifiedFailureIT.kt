package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import br.com.itau.challenge.balance.support.EventPayloads
import br.com.itau.challenge.balance.support.KafkaITBase
import br.com.itau.challenge.balance.support.TopicSet
import org.awaitility.kotlin.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
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

        /** Thread do consumidor que executou a entrega mais recente de cada conta marcada (`<listener>-<indice>-C-1`). */
        val lastThread = ConcurrentHashMap<String, String>()

        override fun applyIfNewer(snapshot: BalanceSnapshot): ApplyResult {
            val account = snapshot.accountId.value
            if (account !in poisoned) return delegate.applyIfNewer(snapshot)
            lastThread[account] = Thread.currentThread().name
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
        topics.awaitGroupLagZero()
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

    /**
     * A contagem de entregas e guardada POR CONSUMIDOR (`DefaultErrorHandler`): se o dono da particao sai do grupo no meio das 3
     * entregas (rebalance, deploy, falha do pod), o novo dono recomeca a contagem e o registro tem MAIS de 3 entregas. E o at-least-once
     * funcionando, nao um defeito: o contrato e "no minimo 3 entregas, DLT exatamente uma vez, desfecho contado exatamente uma vez".
     * O consumidor dono e removido de forma deterministica (`stop()` do container filho) depois da primeira entrega; era a causa do
     * `error=5` (em vez de 3) que o `ObservabilityIT` viu no runner do CI, onde consumidores entram no grupo com atraso.
     */
    @Test
    fun `a consumer leaving the group in the middle of the deliveries restarts the count but the dlt still gets the message exactly once`() {
        val before = topics.dltEndOffsets()
        val rejectedBefore = rejected("unprocessable_event")
        val poisoned = newAccount()
        val neighbour = newAccount()
        writer.poisoned += poisoned
        val poisonedPayload = EventPayloads.transaction(poisoned, balanceAmount = "14.00")

        topics.publishKeyed("mesma-particao-rebalance", poisonedPayload)
        topics.publishKeyed("mesma-particao-rebalance", EventPayloads.transaction(neighbour, balanceAmount = "77.00"))

        // a primeira entrega falhou e a proxima so vem depois do backoff: e a janela para tirar o dono do grupo
        await.pollInterval(Duration.ofMillis(2)).atMost(Duration.ofSeconds(10)).until { (writer.attempts[poisoned]?.get() ?: 0) >= 1 }
        val ownerThread = assertNotNull(writer.lastThread[poisoned])
        val owner =
            assertNotNull(
                registry.listenerContainers
                    .filterIsInstance<ConcurrentMessageListenerContainer<*, *>>()
                    .flatMap { it.containers }
                    .firstOrNull { ownerThread.startsWith("${it.beanName}-C-") },
                "container filho dono de $ownerThread",
            )
        val attemptsBeforeLeaving = writer.attempts.getValue(poisoned).get()
        owner.stop()
        try {
            awaitBalance(neighbour, "77.00")
            topics.awaitGroupLagZero()
        } finally {
            owner.start()
        }
        val deliveries = writer.attempts.getValue(poisoned).get()
        assertTrue(deliveries >= 3, "no minimo 3 entregas antes do DLT: $deliveries")
        assertTrue(deliveries > 3, "o dono saiu depois de $attemptsBeforeLeaving entrega(s) e o novo dono recomecou a contagem, logo passa de 3: $deliveries")
        assertEquals(1, topics.dltRecordsSince(before).size, "o DLT recebe a mensagem exatamente uma vez")
        assertEquals(rejectedBefore + 1, rejected("unprocessable_event"), "o desfecho e contado exatamente uma vez")
        assertEquals(404, get(poisoned).statusCode(), "a conta com defeito nunca foi gravada")
    }

    companion object {
        private val topicSet = TopicSet("it-unclassified")

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) = topicSet.registerProperties(registry)
    }
}
