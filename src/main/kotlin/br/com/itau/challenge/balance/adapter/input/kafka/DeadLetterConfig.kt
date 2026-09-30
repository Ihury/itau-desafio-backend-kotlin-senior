package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.port.output.ProcessingMetrics
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.slf4j.MDC
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
 * Propriedades `balance.dlt.*`. [waitForSendResultTimeout] e o tempo maximo que a publicacao sincrona no DLT espera pela
 * confirmacao do broker.
 */
@ConfigurationProperties("balance.dlt")
class DeadLetterProperties(
    val waitForSendResultTimeout: Duration,
)

/**
 * Unico `CommonErrorHandler` do contexto (bean `kafkaErrorHandler`); classifica cada falha por classe ([FailureClassifier]):
 *
 * - Permanente ([InvalidEventException]): nao retentavel; vai ao DLT na primeira falha, com os bytes originais e os headers
 *   `x-rejection-*` (nenhum header de excecao do Spring, que poderia trazer trechos do payload).
 * - Transitoria ([BalanceStoreUnavailableException]): backoff exponencial ([BackOffProperties]) sem limite de tentativas nem de
 *   tempo, com o container pausado durante a espera ([BackpressureConfig]). O recoverer nunca e acionado: a mensagem valida
 *   fica no broker e jamais chega ao DLT. Cada entrega que falha conta `balance.consumer.backpressure{cause}`.
 * - Nao classificada (qualquer outra): 3 entregas (`FixedBackOff(100 ms, 2)`) e DLT `unprocessable_event`, para que um
 *   defeito deterministico nao bloqueie a particao para sempre.
 *
 * A publicacao no DLT e sincrona (`waitForSendResultTimeout`, `max.block.ms`, `acks=all`, idempotencia). Se ela falhar, o
 * recoverer lanca, o offset nao e confirmado e o registro e reentregue (o container segue vivo); a falha e contada em
 * `balance.dlt.publish.failures`. `rejected{reason}` so e contado no `RetryListener.recovered`, isto e, depois que o DLT
 * confirma.
 */
@Configuration
@EnableConfigurationProperties(DeadLetterProperties::class)
class DeadLetterConfig {
    /** Produtor do DLT: bytes verbatim; `acks`, idempotencia e `max.block.ms` vem de `spring.kafka.producer`. */
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
        backOff: BackOffProperties,
        containerPausingBackOffHandler: BackOffHandler,
    ): CommonErrorHandler =
        deadLetterErrorHandler(deadLetterKafkaTemplate, dltTopic, properties.waitForSendResultTimeout, clock, metrics, backOff, containerPausingBackOffHandler)

    /** [backOffHandler] e parametro para os testes trocarem a pausa real por um que so registra o intervalo. */
    internal fun deadLetterErrorHandler(
        template: KafkaOperations<ByteArray, ByteArray>,
        dltTopic: String,
        waitForSendResultTimeout: Duration,
        clock: Clock,
        metrics: ProcessingMetrics,
        backOff: BackOffProperties,
        backOffHandler: BackOffHandler,
    ): DefaultErrorHandler {
        val recoverer = deadLetterRecoverer(template, dltTopic, waitForSendResultTimeout, RejectionHeaders(clock))
        // O backoff padrao (o da transitoria) NUNCA e STOP: com um padrao que esgota, o Spring pularia a funcao por classe e
        // recuperaria toda falha na primeira entrega.
        val handler = DefaultErrorHandler(recoverer, transientBackOff(backOff), backOffHandler)
        // So o evento invalido e nao retentavel. Todo o resto e retentavel, inclusive o que o Spring Kafka classifica como fatal
        // por padrao (conversao, `ClassCastException`): iria direto ao DLT sem as 3 entregas.
        handler.setClassifications(mapOf(InvalidEventException::class.java to false), true)
        handler.setBackOffFunction { _, failure -> backOffFor(failure, backOff) }
        handler.setRetryListeners(DeadLetterRetryListener(metrics))
        return handler
    }

    private fun deadLetterRecoverer(
        template: KafkaOperations<ByteArray, ByteArray>,
        dltTopic: String,
        waitForSendResultTimeout: Duration,
        rejectionHeaders: RejectionHeaders,
    ): DeadLetterPublishingRecoverer {
        // Particao -1: o particionador escolhe (o padrao "mesma particao" falharia com 12 -> 3 particoes).
        val recoverer = DeadLetterPublishingRecoverer(template) { _, _ -> TopicPartition(dltTopic, -1) }
        recoverer.setHeadersFunction { _, failure ->
            val rejection = FailureClassifier.rejectionOf(failure)
            rejectionHeaders.of(rejection.reason, rejection.fieldPath)
        }
        // Mensagens de excecao de parsers podem conter trechos do payload: o DLT nao amplia essa superficie.
        recoverer.excludeHeader(HeadersToAdd.EXCEPTION, HeadersToAdd.EX_CAUSE, HeadersToAdd.EX_MSG, HeadersToAdd.EX_STACKTRACE)
        // Com particao nao definida nao ha o que verificar (e a verificacao consultaria os metadados de um topico ausente).
        recoverer.setVerifyPartition(false)
        recoverer.setFailIfSendResultIsError(true)
        recoverer.setWaitForSendResultTimeout(waitForSendResultTimeout)
        // Por padrao o Spring espera `delivery.timeout.ms + 5 s` (>= 125 s), ignorando `waitForSendResultTimeout` se menor.
        recoverer.setTimeoutBuffer(0)
        return recoverer
    }

    /**
     * Exponencial com jitter nativo do Spring Framework 7 e sem limite de tentativas nem de tempo: o `ExponentialBackOff` so
     * esgota se `maxAttempts` ou `maxElapsedTime` forem limitados.
     */
    internal fun transientBackOff(settings: BackOffProperties): ExponentialBackOff =
        ExponentialBackOff(settings.initialMs, TRANSIENT_MULTIPLIER).apply {
            maxInterval = settings.maxMs
            jitter = settings.jitterMs
        }

    internal fun unclassifiedBackOff(): FixedBackOff = FixedBackOff(UNCLASSIFIED_INTERVAL_MS, UNCLASSIFIED_RETRIES)

    internal fun backOffFor(
        failure: Exception,
        settings: BackOffProperties,
    ): BackOff =
        when (FailureClassifier.classify(failure)) {
            FailureClass.PERMANENT -> FixedBackOff(NO_RETRY, NO_RETRY)
            FailureClass.TRANSIENT -> transientBackOff(settings)
            FailureClass.UNCLASSIFIED -> unclassifiedBackOff()
        }

    /**
     * Nunca registra payload, saldo, titular nem texto de excecao: so coordenadas do registro, classe da excecao e motivo. O
     * MDC `correlationId` (mesmo formato do listener) vale so durante cada log.
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
            correlated(record) {
                when (failure) {
                    is BalanceStoreUnavailableException -> {
                        metrics.backpressure(failure.failureCause)
                        // Sem a mensagem livre do SDK nem payload. MISCONFIGURED sobe a ERROR: continua transitoria, mas exige acao
                        // de quem opera.
                        if (failure.failureCause == StoreFailureCause.MISCONFIGURED) {
                            log.error("store unavailable, container paused for the back off {} attempt={} {}", coordinates(record), deliveryAttempt, failure.logDescription())
                        } else {
                            log.warn("store unavailable, container paused for the back off {} attempt={} {}", coordinates(record), deliveryAttempt, failure.logDescription())
                        }
                    }
                    // Evento invalido e um desfecho esperado (permanente, sem reentrega): o isolamento e logado em `recovered`.
                    is InvalidEventException -> log.debug("invalid event {} reason={}", coordinates(record), failure.reason.code)
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
        }

        override fun recovered(
            record: ConsumerRecord<*, *>,
            exception: Exception?,
        ) {
            val rejection = FailureClassifier.rejectionOf(exception ?: IllegalStateException())
            metrics.rejected(rejection.reason)
            correlated(record) {
                log.warn("message isolated in the dlt {} reason={} detail={}", coordinates(record), rejection.reason.code, rejection.fieldPath)
            }
        }

        override fun recoveryFailed(
            record: ConsumerRecord<*, *>,
            original: Exception?,
            failure: Exception,
        ) {
            metrics.dltPublishFailed()
            correlated(record) {
                log.error("dlt publication failed, record not confirmed and will be redelivered {} exception={}", coordinates(record), failure.javaClass.name)
            }
        }

        private fun coordinates(record: ConsumerRecord<*, *>) = "${record.topic()}-${record.partition()}@${record.offset()}"

        private fun correlated(
            record: ConsumerRecord<*, *>,
            block: () -> Unit,
        ) {
            MDC.put(CORRELATION_ID, coordinates(record))
            try {
                block()
            } finally {
                MDC.remove(CORRELATION_ID)
            }
        }
    }

    private companion object {
        const val TRANSIENT_MULTIPLIER = 2.0
        const val NO_RETRY = 0L
        const val UNCLASSIFIED_INTERVAL_MS = 100L
        const val UNCLASSIFIED_RETRIES = 2L
        const val CORRELATION_ID = "correlationId"
        private val log = LoggerFactory.getLogger(DeadLetterConfig::class.java)
    }
}
