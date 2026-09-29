package br.com.itau.challenge.balance.adapter.input.kafka

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.listener.BackOffHandler
import org.springframework.kafka.listener.CommonErrorHandler
import org.springframework.kafka.listener.ConsumerRecordRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.ExponentialBackOff

/**
 * Error handler do consumer SEM descarte (Constitution III): nenhum registro que falha e pulado nem tem o offset
 * confirmado sem persistencia. O `DefaultErrorHandler` padrao do Spring Kafka (`FixedBackOff(0, 9)` seguido de log e skip)
 * e exatamente o que esta configuracao substitui.
 *
 * - `ExponentialBackOff` de 500 ms, multiplicador 2,0, teto de 30 s e SEM limite de tentativas nem de tempo: o backoff
 *   nunca esgota e nenhuma espera alcanca o `max.poll.interval.ms`.
 * - O recoverer nunca confirma o offset: se algum dia for chamado, lanca e o registro continua nao confirmado.
 * - Todas as excecoes sao retentaveis: as que o Spring Kafka classifica como fatais por padrao (conversao, `ClassCastException`,
 *   `NoSuchMethodException`) iriam direto ao recoverer, sem espera.
 *
 * Enquanto nao ha DLT, mensagem invalida ou falha transitoria fica retida e e reentregue com backoff, nunca descartada.
 * O `DeadLetterConfig` da US4 substitui este bean (mesmo nome, um unico `CommonErrorHandler` ativo) e passa a classificar por
 * excecao. O Spring Boot liga o unico bean `CommonErrorHandler` ao container do listener.
 */
@Configuration
class FailSafeErrorHandlerConfig {
    @Bean
    fun kafkaErrorHandler(): CommonErrorHandler = failSafeErrorHandler()

    internal fun failSafeBackOff(): ExponentialBackOff =
        ExponentialBackOff(INITIAL_INTERVAL_MS, MULTIPLIER).apply { maxInterval = MAX_INTERVAL_MS }

    /** Recoverer que se recusa a confirmar: descartar mensagem e proibido. */
    internal val neverRecovers: ConsumerRecordRecoverer =
        ConsumerRecordRecoverer { _, _ -> throw IllegalStateException("descartar mensagem e proibido: o offset nao pode ser confirmado sem persistencia") }

    /** [backOffHandler] so e informado por testes, para observar a espera sem dormir; producao usa o padrao do Spring Kafka. */
    internal fun failSafeErrorHandler(backOffHandler: BackOffHandler? = null): DefaultErrorHandler {
        val handler =
            if (backOffHandler == null) {
                DefaultErrorHandler(neverRecovers, failSafeBackOff())
            } else {
                DefaultErrorHandler(neverRecovers, failSafeBackOff(), backOffHandler)
            }
        handler.setClassifications(emptyMap(), true)
        return handler
    }

    private companion object {
        const val INITIAL_INTERVAL_MS = 500L
        const val MULTIPLIER = 2.0
        const val MAX_INTERVAL_MS = 30_000L
    }
}
