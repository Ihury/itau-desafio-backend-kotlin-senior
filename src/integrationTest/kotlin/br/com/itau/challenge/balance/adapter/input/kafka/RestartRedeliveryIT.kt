package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.Application
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import br.com.itau.challenge.balance.support.DynamoDbTestSupport
import br.com.itau.challenge.balance.support.EventPayloads
import br.com.itau.challenge.balance.support.IntegrationInfra
import br.com.itau.challenge.balance.support.TopicSet
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.context.TypeExcludeFilter
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.core.type.classreading.MetadataReader
import org.springframework.core.type.classreading.MetadataReaderFactory
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes
import java.math.BigDecimal
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Reinicio sem perda: o contexto A consome o topico e e ENCERRADO DE FORMA GRACIOSA (`close`, o mesmo caminho do SIGTERM) no
 * meio; o contexto B, do mesmo grupo, retoma de onde o grupo confirmou (`immediate-stop`, commit so pelo container): B processa
 * exatamente o que A nunca escreveu e qualquer excedente reentregue vira `duplicate` (nunca uma segunda escrita nem um saldo
 * errado). Cada mensagem consumida por B tem exatamente um desfecho; ao final existem exatamente 500 itens, cada um com o saldo
 * do seu evento, e nenhum evento no DLT. Depois o grupo volta ao inicio (a reentrega total que uma queda sem confirmacao
 * provocaria) e o contexto C reconcilia as 500 mensagens como `duplicate`, sem alterar nenhum item.
 *
 * Os contextos sao criados programaticamente (`SpringApplicationBuilder`) para poderem ser encerrados e recriados; as
 * `@TestConfiguration` de outros ITs no classpath ficam de fora da varredura (o `TestTypeExcludeFilter` so existe em `@SpringBootTest`).
 */
class RestartRedeliveryIT {
    /** Atrasa cada escrita do contexto A: com 4 threads de consumo ele leva alguns segundos para 500 eventos, e o desligamento chega no meio. */
    @TestConfiguration
    class SlowWriterConfig {
        @Bean
        @Primary
        fun slowWriter(
            @Qualifier("balanceSnapshotWriter") delegate: BalanceSnapshotWriter,
        ): BalanceSnapshotWriter =
            object : BalanceSnapshotWriter {
                override fun applyIfNewer(snapshot: BalanceSnapshot): ApplyResult {
                    Thread.sleep(WRITE_DELAY_MS)
                    return delegate.applyIfNewer(snapshot)
                }
            }
    }

    /** Fora da varredura de componentes toda `@TestConfiguration` (as de outros ITs); so as fontes explicitas entram. */
    private class ExcludeTestConfigurations : TypeExcludeFilter() {
        override fun match(
            metadataReader: MetadataReader,
            metadataReaderFactory: MetadataReaderFactory,
        ): Boolean = metadataReader.annotationMetadata.isAnnotated(TestConfiguration::class.java.name)
    }

    private fun start(
        topics: TopicSet,
        vararg sources: Class<*>,
    ): ConfigurableApplicationContext =
        SpringApplicationBuilder(Application::class.java, *sources)
            .initializers(
                ApplicationContextInitializer<ConfigurableApplicationContext> { it.beanFactory.registerSingleton("excludeTestConfigurations", ExcludeTestConfigurations()) },
            // argumentos de linha de comando: precedencia sobre o `application.yaml` (`properties(...)` seria so o padrao, o mais baixo)
            ).run(*(topics.properties() + mapOf("server.port" to "0", "spring.main.banner-mode" to "off")).map { (name, value) -> "--$name=$value" }.toTypedArray())

    /** Total de `balance.events` do desfecho (o registry continua legivel depois de o contexto fechar). */
    private fun MeterRegistry.total(outcome: String): Int = find("balance.events").tag("outcome", outcome).counters().sumOf { it.count().toInt() }

    private fun outcomes(registry: MeterRegistry): Map<String, Int> = listOf("processed", "obsolete", "duplicate", "rejected").associateWith { registry.total(it) }

    @Test
    fun `a graceful shutdown in the middle of the consumption loses nothing and the redeliveries are counted as duplicates`() {
        val topics = TopicSet("it-restart")
        val accounts = (1..EVENTS).map { DynamoDbTestSupport.randomAccountId() }
        val raw = DynamoDbTestSupport.rawClient()
        val contexts = mutableListOf<ConfigurableApplicationContext>()

        fun stored(): Map<String, BigDecimal> =
            accounts
                .chunked(100)
                .flatMap { chunk ->
                    val keys = KeysAndAttributes.builder().keys(chunk.map { DynamoDbTestSupport.key(it) }).consistentRead(true).build()
                    raw.batchGetItem(BatchGetItemRequest.builder().requestItems(mapOf(DynamoDbTestSupport.tableName to keys)).build()).responses()[DynamoDbTestSupport.tableName].orEmpty()
                }.associate { it.getValue("pk").s().removePrefix("ACCOUNT#") to BigDecimal(it.getValue("balanceAmount").n()) }

        try {
            val a = start(topics, SlowWriterConfig::class.java).also { contexts += it }
            val metricsOfA = a.getBean(MeterRegistry::class.java)
            topics.awaitAssignment(a.getBean(KafkaListenerEndpointRegistry::class.java))
            val dltBefore = topics.dltEndOffsets()
            // sem chave (espalhados pelas particoes), cada evento com o saldo do seu indice
            accounts.forEachIndexed { index, account -> topics.publish(EventPayloads.transaction(account, balanceAmount = "${index + 1}.00")) }

            await.atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(20)).until { metricsOfA.total("processed") >= SHUTDOWN_AFTER_PROCESSED }
            a.close()
            contexts.remove(a)
            val processedByA = metricsOfA.total("processed")
            println("RESTART processedByA=$processedByA de $EVENTS")
            assertTrue(processedByA in 1 until EVENTS, "o desligamento chegou no meio do consumo: A processou $processedByA de $EVENTS")
            assertEquals(processedByA, stored().size, "cada evento processado por A esta gravado (nada foi confirmado sem persistir)")
            val lagBeforeB = topics.groupLag()
            assertTrue(lagBeforeB >= EVENTS - processedByA, "o que A nao processou continua sem confirmacao: lag $lagBeforeB")

            val b = start(topics).also { contexts += it }
            topics.awaitAssignment(b.getBean(KafkaListenerEndpointRegistry::class.java))
            topics.awaitGroupLagZero(Duration.ofSeconds(90))
            await.atMost(Duration.ofSeconds(30)).untilAsserted { assertEquals(EVENTS, stored().size, "itens gravados") }

            val counts = outcomes(b.getBean(MeterRegistry::class.java))
            println("RESTART lagBeforeB=$lagBeforeB outcomes(B)=$counts")
            assertEquals(lagBeforeB.toInt(), counts.getValue("processed") + counts.getValue("duplicate") + counts.getValue("obsolete"), "cada mensagem consumida por B tem exatamente um desfecho")
            assertEquals(EVENTS - processedByA, counts.getValue("processed"), "B processa exatamente o que A nunca escreveu")
            // num encerramento gracioso o container confirma o que A ja processou: o excedente de lagBeforeB e a reentrega de A
            assertEquals(lagBeforeB.toInt() - (EVENTS - processedByA), counts.getValue("duplicate"), "a reentrega do que A ja escrevera e `duplicate`")
            assertEquals(0, counts.getValue("obsolete"))
            assertEquals(0, counts.getValue("rejected"))
            assertEquals(0, topics.dltCountSince(dltBefore), "nenhum evento no DLT")
            val balances = stored()
            assertEquals(EVENTS, balances.size, "exatamente $EVENTS itens")
            accounts.forEachIndexed { index, account -> assertEquals(0, BigDecimal("${index + 1}.00").compareTo(balances.getValue(account)), "saldo da conta ${index + 1}") }

            // reentrega total (o que uma queda sem confirmacao provocaria): B encerra, o grupo volta ao inicio e C consome tudo de novo
            b.close()
            contexts.remove(b)
            rewindGroup(topics)
            assertEquals(EVENTS.toLong(), topics.groupLag(), "o grupo voltou ao inicio: as $EVENTS mensagens serao reentregues")
            val c = start(topics).also { contexts += it }
            topics.awaitAssignment(c.getBean(KafkaListenerEndpointRegistry::class.java))
            topics.awaitGroupLagZero(Duration.ofSeconds(90))
            val redelivered = outcomes(c.getBean(MeterRegistry::class.java))
            println("RESTART outcomes(C)=$redelivered")
            assertEquals(mapOf("processed" to 0, "obsolete" to 0, "duplicate" to EVENTS, "rejected" to 0), redelivered, "toda reentrega vira `duplicate`: nenhuma escrita a mais, nenhum saldo trocado")
            assertEquals(balances, stored(), "os itens seguem identicos")
            assertEquals(0, topics.dltCountSince(dltBefore), "nenhum evento no DLT")
        } finally {
            contexts.forEach { runCatching { it.close() } }
            accounts.forEach { runCatching { raw.deleteItem(DeleteItemRequest.builder().tableName(DynamoDbTestSupport.tableName).key(DynamoDbTestSupport.key(it)).build()) } }
            raw.close()
        }
    }

    /** Volta o grupo (sem membros) ao inicio do topico: simula a reentrega de tudo que nao chegou a ser confirmado. */
    private fun rewindGroup(topics: TopicSet) {
        IntegrationInfra.adminClient().use { admin ->
            val zero = (0 until IntegrationInfra.MAIN_PARTITIONS).associate { TopicPartition(topics.topic, it) to OffsetAndMetadata(0L) }
            // o grupo so fica vazio depois que o ultimo membro sai (o `close` envia o LeaveGroup)
            await.atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(300)).untilAsserted {
                admin.alterConsumerGroupOffsets(topics.groupId, zero).all().get(15, TimeUnit.SECONDS)
            }
        }
    }

    private companion object {
        const val EVENTS = 500
        const val SHUTDOWN_AFTER_PROCESSED = 80
        const val WRITE_DELAY_MS = 20L

        init {
            // o objeto de apoio configura os timeouts de espera padrao do Awaitility
            IntegrationInfra.bootstrapServers
        }
    }
}
