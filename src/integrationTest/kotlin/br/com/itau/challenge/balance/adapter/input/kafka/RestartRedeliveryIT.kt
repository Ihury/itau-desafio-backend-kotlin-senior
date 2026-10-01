package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.Application
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import br.com.itau.challenge.balance.support.AwaitilityDefaults
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
import org.junit.jupiter.api.extension.ExtendWith
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
import java.math.BigDecimal
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@ExtendWith(AwaitilityDefaults::class)
class RestartRedeliveryIT {
    @TestConfiguration
    class SlowWriterConfig {
        @Bean
        @Primary
        fun slowWriter(
            @Qualifier("balanceSnapshotWriter") delegate: BalanceSnapshotWriter,
        ): BalanceSnapshotWriter =
            object : BalanceSnapshotWriter {
                override fun applyIfNewer(snapshot: BalanceSnapshot): ApplyResult {
                    Thread.sleep(SHUTDOWN_RACE_WRITE_DELAY_MS)
                    return delegate.applyIfNewer(snapshot)
                }
            }
    }

    private class ExcludeAllTestConfigurationsFromScan : TypeExcludeFilter() {
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
                ApplicationContextInitializer<ConfigurableApplicationContext> {
                    it.beanFactory.registerSingleton("excludeAllTestConfigurationsFromScan", ExcludeAllTestConfigurationsFromScan())
                },
            // args de CLI vencem o application.yaml; properties(...) perderia.
            ).run(*(topics.properties() + mapOf("server.port" to "0", "spring.main.banner-mode" to "off")).map { (name, value) -> "--$name=$value" }.toTypedArray())

    private fun MeterRegistry.outcomeTotal(outcome: String): Int = find("balance.events").tag("outcome", outcome).counters().sumOf { it.count().toInt() }

    private fun outcomes(registry: MeterRegistry): Map<String, Int> = OUTCOMES.associateWith { registry.outcomeTotal(it) }

    private fun publishOneEventPerAccount(
        topics: TopicSet,
        accounts: List<String>,
    ) {
        accounts.forEachIndexed { index, account -> topics.publish(EventPayloads.transaction(account, balanceAmount = balanceOfEvent(index))) }
    }

    private fun balanceOfEvent(index: Int): String = "${index + 1}.00"

    private fun shutdownGracefullyAfterProcessing(
        consumer: ConfigurableApplicationContext,
        processedThreshold: Int,
    ): Int {
        val metrics = consumer.getBean(MeterRegistry::class.java)
        await.atMost(SHUTDOWN_WAIT).pollInterval(Duration.ofMillis(20)).until { metrics.outcomeTotal("processed") >= processedThreshold }
        consumer.close()
        return metrics.outcomeTotal("processed")
    }

    private fun assertShutdownCameInTheMiddle(processedByA: Int) {
        assertTrue(processedByA in 1 until EVENTS, "o desligamento chegou no meio do consumo: A processou $processedByA de $EVENTS")
    }

    private fun assertBProcessedOnlyWhatAHadNotWritten(
        outcomesOfB: Map<String, Int>,
        lagBeforeB: Long,
        processedByA: Int,
    ) {
        val neverWrittenByA = EVENTS - processedByA
        assertEquals(
            lagBeforeB.toInt(),
            outcomesOfB.getValue("processed") + outcomesOfB.getValue("duplicate") + outcomesOfB.getValue("obsolete"),
            "cada mensagem consumida por B tem exatamente um desfecho",
        )
        assertEquals(neverWrittenByA, outcomesOfB.getValue("processed"), "B processa exatamente o que A nunca escreveu")
        assertEquals(
            lagBeforeB.toInt() - neverWrittenByA,
            outcomesOfB.getValue("duplicate"),
            "num encerramento gracioso o container confirma o que A ja processou: a reentrega do que A ja escrevera e `duplicate`",
        )
        assertEquals(0, outcomesOfB.getValue("obsolete"))
        assertEquals(0, outcomesOfB.getValue("rejected"))
    }

    private fun assertEveryItemHasTheBalanceOfItsEvent(
        balances: Map<String, BigDecimal>,
        accounts: List<String>,
    ) {
        assertEquals(EVENTS, balances.size, "exatamente $EVENTS itens")
        accounts.forEachIndexed { index, account ->
            assertEquals(0, BigDecimal(balanceOfEvent(index)).compareTo(balances.getValue(account)), "saldo da conta ${index + 1}")
        }
    }

    private fun rewindGroupOnceItHasNoMembers(topics: TopicSet) {
        IntegrationInfra.adminClient().use { admin ->
            val zero = (0 until IntegrationInfra.MAIN_PARTITIONS).associate { TopicPartition(topics.topic, it) to OffsetAndMetadata(0L) }
            await.atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(300)).untilAsserted {
                admin.alterConsumerGroupOffsets(topics.groupId, zero).all().get(15, TimeUnit.SECONDS)
            }
        }
    }

    @Test
    fun `a graceful shutdown in the middle of the consumption loses nothing and the redeliveries are counted as duplicates`() {
        val topics = TopicSet("it-restart")
        val accounts = (1..EVENTS).map { DynamoDbTestSupport.randomAccountId() }
        val raw = DynamoDbTestSupport.rawClient()
        val contexts = mutableListOf<ConfigurableApplicationContext>()

        fun stored(): Map<String, BigDecimal> = DynamoDbTestSupport.storedBalances(raw, accounts)

        fun startConsumer(vararg sources: Class<*>): ConfigurableApplicationContext =
            start(topics, *sources).also {
                contexts += it
                topics.group.awaitStabilized(it.getBean(KafkaListenerEndpointRegistry::class.java))
            }

        fun closeConsumer(consumer: ConfigurableApplicationContext) {
            consumer.close()
            contexts.remove(consumer)
        }

        try {
            val consumerA = startConsumer(SlowWriterConfig::class.java)
            val dltBefore = topics.dlt.endOffsets()
            publishOneEventPerAccount(topics, accounts)

            val processedByA = shutdownGracefullyAfterProcessing(consumerA, SHUTDOWN_AFTER_PROCESSED)
            contexts.remove(consumerA)
            assertShutdownCameInTheMiddle(processedByA)
            assertEquals(processedByA, stored().size, "cada evento processado por A esta gravado (nada foi confirmado sem persistir)")
            val lagBeforeB = topics.group.lag()
            assertTrue(lagBeforeB >= EVENTS - processedByA, "o que A nao processou continua sem confirmacao: lag $lagBeforeB")

            val consumerB = startConsumer()
            topics.group.awaitLagZero(Duration.ofSeconds(90))
            await.atMost(Duration.ofSeconds(30)).untilAsserted { assertEquals(EVENTS, stored().size, "itens gravados") }

            assertBProcessedOnlyWhatAHadNotWritten(outcomes(consumerB.getBean(MeterRegistry::class.java)), lagBeforeB, processedByA)
            assertEquals(0, topics.dlt.countSince(dltBefore), "nenhum evento no DLT")
            val balances = stored()
            assertEveryItemHasTheBalanceOfItsEvent(balances, accounts)

            closeConsumer(consumerB)
            rewindGroupOnceItHasNoMembers(topics)
            assertEquals(EVENTS.toLong(), topics.group.lag(), "o grupo voltou ao inicio: as $EVENTS mensagens serao reentregues")
            val consumerC = startConsumer()
            topics.group.awaitLagZero(Duration.ofSeconds(90))
            val redelivered = outcomes(consumerC.getBean(MeterRegistry::class.java))
            assertEquals(
                mapOf("processed" to 0, "obsolete" to 0, "duplicate" to EVENTS, "rejected" to 0),
                redelivered,
                "toda reentrega vira `duplicate`: nenhuma escrita a mais, nenhum saldo trocado",
            )
            assertEquals(balances, stored(), "os itens seguem identicos")
            assertEquals(0, topics.dlt.countSince(dltBefore), "nenhum evento no DLT")
        } finally {
            contexts.forEach { runCatching { it.close() } }
            accounts.forEach { runCatching { DynamoDbTestSupport.deleteAccount(raw, it) } }
            raw.close()
        }
    }

    private companion object {
        const val EVENTS = 500
        const val SHUTDOWN_AFTER_PROCESSED = 80
        const val SHUTDOWN_RACE_WRITE_DELAY_MS = 20L
        val SHUTDOWN_WAIT: Duration = Duration.ofSeconds(60)
        val OUTCOMES = listOf("processed", "obsolete", "duplicate", "rejected")
    }
}
