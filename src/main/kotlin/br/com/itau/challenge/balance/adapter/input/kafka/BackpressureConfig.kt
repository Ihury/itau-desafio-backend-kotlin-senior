package br.com.itau.challenge.balance.adapter.input.kafka

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.listener.BackOffHandler
import org.springframework.kafka.listener.ContainerPausingBackOffHandler
import org.springframework.kafka.listener.ListenerContainerPauseService
import org.springframework.scheduling.TaskScheduler
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler

/**
 * Parametros do backoff da falha transitoria (`balance.consumer.backoff.*`; variaveis `KAFKA_BACKOFF_INITIAL_MS`,
 * `KAFKA_BACKOFF_MAX_MS` e `KAFKA_BACKOFF_JITTER_MS` em `contracts/configuration.md`; os defaults vivem no `application.yaml`).
 * O multiplicador (2,0) e fixo. [jitterMs] e o jitter do intervalo inicial e escala com o multiplicador (Spring Framework 7).
 */
@ConfigurationProperties("balance.consumer.backoff")
data class BackOffProperties(
    val initialMs: Long,
    val maxMs: Long,
    val jitterMs: Long,
)

/**
 * Backpressure da falha transitoria (FR-028, FR-029): durante a espera do backoff o container fica PAUSADO (o poll continua
 * vivo, entao nao ha rebalance por `max.poll.interval.ms`) e a mensagem valida permanece no broker. A retomada e agendada num
 * `TaskScheduler` proprio, nunca por `Thread.sleep` no thread do poll (research.md R-08).
 */
@Configuration
@EnableConfigurationProperties(BackOffProperties::class)
class BackpressureConfig {
    /** Agenda a retomada dos containers pausados; um thread basta (cada tarefa so chama `container.resume()`). */
    @Bean
    fun kafkaBackOffScheduler(): ThreadPoolTaskScheduler =
        ThreadPoolTaskScheduler().apply {
            setPoolSize(1)
            setThreadNamePrefix("kafka-backoff-")
            setDaemon(true)
            setRemoveOnCancelPolicy(true)
        }

    /** Pausa o container (o filho que falhou, um por thread de consumo) durante o backoff e o retoma ao fim dele. */
    @Bean
    fun containerPausingBackOffHandler(kafkaBackOffScheduler: TaskScheduler): BackOffHandler =
        // Sem registro: so se pausa o proprio container recebido do error handler (a pausa por id exigiria o registro).
        ContainerPausingBackOffHandler(ListenerContainerPauseService(null, kafkaBackOffScheduler))
}
