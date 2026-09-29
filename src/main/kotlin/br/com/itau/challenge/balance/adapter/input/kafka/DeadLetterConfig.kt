package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.port.output.ProcessingMetrics
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.BackOffHandler
import org.springframework.kafka.listener.CommonErrorHandler
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.listener.RetryListener
import org.springframework.util.backoff.BackOff
import org.springframework.util.backoff.ExponentialBackOff
import org.springframework.util.backoff.FixedBackOff
import java.time.Clock
import java.time.Duration

/**
 * Propriedades `balance.dlt.*` (contracts/configuration.md; os defaults vivem no `application.yaml`). [waitForSendResultTimeout]
 * e o tempo maximo que a publicacao SINCRONA no DLT espera pela confirmacao do broker.
 */
@ConfigurationProperties("balance.dlt")
class DeadLetterProperties(
    val waitForSendResultTimeout: Duration,
)

/**
 * Tratamento de erros do consumer (US4): o UNICO `CommonErrorHandler` do contexto (bean `kafkaErrorHandler`), que substitui o
 * handler sem descarte da US2 e classifica cada falha por CLASSE (research.md R-08, [FailureClassifier]):
 *
 * - **Permanente** ([InvalidEventException]): nao retentavel; vai ao DLT na primeira falha, com os bytes originais e os headers
 *   `x-rejection-*` (nenhum header de excecao do Spring, que poderia trazer trechos do payload).
 * - **Transitoria** ([BalanceStoreUnavailableException]): `ExponentialBackOff` (500 ms x2, teto de 30 s) SEM limite de tentativas
 *   nem de tempo; o recoverer NUNCA e acionado, a mensagem valida fica no broker e jamais chega ao DLT (FR-017).
 * - **Nao classificada** (qualquer outra): 3 entregas (`FixedBackOff(100 ms, 2)`) e DLT `unprocessable_event`, para que um
 *   defeito deterministico nao bloqueie a particao para sempre (Constitution III).
 *
 * A publicacao no DLT e sincrona (`waitForSendResultTimeout`, `max.block.ms`, `acks=all`, idempotencia). Se ela falhar, o
 * recoverer lanca, o offset NAO e confirmado e o registro e reentregue (o container segue vivo); a falha e contada em
 * `balance.dlt.publish.failures`. `rejected{reason}` so e contado no `RetryListener.recovered`, isto e, depois que o DLT confirma.
 * O `DefaultErrorHandler` padrao do Spring Kafka (`FixedBackOff(0, 9)` seguido de log e skip) e exatamente o que isto substitui.
 */
@Configuration
@EnableConfigurationProperties(DeadLetterProperties::class)
class DeadLetterConfig {
    /** Produtor do DLT: bytes verbatim (chave e valor), com `acks`, idempotencia e `max.block.ms` vindos de `spring.kafka.producer`. */
    @Bean
    fun deadLetterKafkaTemplate(producerFactory: ProducerFactory<*, *>): KafkaTemplate<ByteArray, ByteArray> {
        @Suppress("UNCHECKED_CAST")
        return KafkaTemplate(producerFactory as ProducerFactory<ByteArray, ByteArray>)
    }

    @Bean
    fun kafkaErrorHandler(
        deadLetterKafkaTemplate: KafkaTemplate<ByteArray, ByteArray>,
        @Value($$"${balance.events.dlt-topic}") dltTopic: String,
        properties: DeadLetterProperties,
        clock: Clock,
        metrics: ProcessingMetrics,
    ): CommonErrorHandler = deadLetterErrorHandler(deadLetterKafkaTemplate, dltTopic, properties.waitForSendResultTimeout, clock, metrics)

    /** [backOffHandler] so e informado por testes, para observar a espera sem dormir; producao usa o padrao do Spring Kafka. */
    internal fun deadLetterErrorHandler(
        template: KafkaOperations<ByteArray, ByteArray>,
        dltTopic: String,
        sendTimeout: Duration,
        clock: Clock,
        metrics: ProcessingMetrics,
        backOffHandler: BackOffHandler? = null,
    ): DefaultErrorHandler {
        val recoverer = deadLetterRecoverer(template, dltTopic, sendTimeout, RejectionHeaders(clock))
        // O backoff padrao (o da transitoria) NUNCA e STOP: com um padrao que esgota, o Spring pularia a funcao por classe e
        // recuperaria toda falha na primeira entrega.
        val handler =
            if (backOffHandler == null) {
                DefaultErrorHandler(recoverer, transientBackOff())
            } else {
                DefaultErrorHandler(recoverer, transientBackOff(), backOffHandler)
            }
        // So o evento invalido e nao retentavel. Todo o resto e retentavel, inclusive o que o Spring Kafka classifica como fatal
        // por padrao (conversao, `ClassCastException`): iria direto ao DLT sem as 3 entregas.
        handler.setClassifications(mapOf(InvalidEventException::class.java to false), true)
        handler.setBackOffFunction { _, failure -> backOffFor(failure) }
        handler.setRetryListeners(DeadLetterRetryListener(metrics))
        return handler
    }

    private fun deadLetterRecoverer(
        template: KafkaOperations<ByteArray, ByteArray>,
        dltTopic: String,
        sendTimeout: Duration,
        rejectionHeaders: RejectionHeaders,
    ): DeadLetterPublishingRecoverer {
        // Particao -1: o particionador escolhe (o padrao "mesma particao" falharia com 12 -> 3 particoes).
        val recoverer = DeadLetterPublishingRecoverer(template) { _, _ -> TopicPartition(dltTopic, -1) }
        recoverer.setHeadersFunction { _, failure ->
            val rejection = FailureClassifier.rejectionOf(failure)
            rejectionHeaders.of(rejection.reason, rejection.detail)
        }
        // Mensagens de excecao de parsers podem conter trechos do payload: o DLT nao amplia essa superficie.
        recoverer.excludeHeader(HeadersToAdd.EXCEPTION, HeadersToAdd.EX_CAUSE, HeadersToAdd.EX_MSG, HeadersToAdd.EX_STACKTRACE)
        // Com particao nao definida nao ha o que verificar (e a verificacao consultaria os metadados de um topico ausente).
        recoverer.setVerifyPartition(false)
        recoverer.setFailIfSendResultIsError(true)
        recoverer.setWaitForSendResultTimeout(sendTimeout)
        // Por padrao o Spring espera `delivery.timeout.ms + 5 s` (>= 125 s), ignorando `waitForSendResultTimeout` se menor.
        recoverer.setTimeoutBuffer(0)
        return recoverer
    }

    internal fun transientBackOff(): ExponentialBackOff = ExponentialBackOff(TRANSIENT_INITIAL_MS, TRANSIENT_MULTIPLIER).apply { maxInterval = TRANSIENT_MAX_MS }

    internal fun unclassifiedBackOff(): FixedBackOff = FixedBackOff(UNCLASSIFIED_INTERVAL_MS, UNCLASSIFIED_RETRIES)

    internal fun backOffFor(failure: Exception): BackOff =
        when (FailureClassifier.classify(failure)) {
            FailureClass.PERMANENT -> FixedBackOff(0L, 0L)
            FailureClass.TRANSIENT -> transientBackOff()
            FailureClass.UNCLASSIFIED -> unclassifiedBackOff()
        }

    /**
     * Metricas e logs do ciclo de falha. NUNCA registra payload, saldo, titular nem texto de excecao: so coordenadas do
     * registro, classe da excecao e motivo.
     */
    private class DeadLetterRetryListener(
        private val metrics: ProcessingMetrics,
    ) : RetryListener {
        override fun failedDelivery(
            record: ConsumerRecord<*, *>,
            exception: Exception?,
            deliveryAttempt: Int,
        ) {
            val failure = exception?.let { FailureClassifier.unwrap(it) }
            when (failure) {
                is BalanceStoreUnavailableException ->
                    log.warn("store unavailable, record kept for redelivery {} attempt={} cause={}", coordinates(record), deliveryAttempt, failure.failureCause)
                else ->
                    log.error(
                        "unclassified failure {} attempt={} exception={} at={}",
                        coordinates(record),
                        deliveryAttempt,
                        failure?.javaClass?.name,
                        failure?.stackTrace?.firstOrNull(),
                    )
            }
        }

        override fun recovered(
            record: ConsumerRecord<*, *>,
            exception: Exception?,
        ) {
            val rejection = FailureClassifier.rejectionOf(exception ?: IllegalStateException())
            metrics.rejected(rejection.reason)
            log.warn("message isolated in the dlt {} reason={} detail={}", coordinates(record), rejection.reason.code, rejection.detail)
        }

        override fun recoveryFailed(
            record: ConsumerRecord<*, *>,
            original: Exception?,
            failure: Exception,
        ) {
            metrics.dltPublishFailed()
            log.error("dlt publication failed, record not confirmed and will be redelivered {} exception={}", coordinates(record), failure.javaClass.name)
        }

        private fun coordinates(record: ConsumerRecord<*, *>) = "${record.topic()}-${record.partition()}@${record.offset()}"
    }

    private companion object {
        const val TRANSIENT_INITIAL_MS = 500L
        const val TRANSIENT_MULTIPLIER = 2.0
        const val TRANSIENT_MAX_MS = 30_000L
        const val UNCLASSIFIED_INTERVAL_MS = 100L
        const val UNCLASSIFIED_RETRIES = 2L
        private val log = LoggerFactory.getLogger(DeadLetterConfig::class.java)
    }
}
