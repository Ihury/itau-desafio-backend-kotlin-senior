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
 * Parametros do backoff da falha transitoria (`balance.consumer.backoff.*`). O multiplicador (2,0) e fixo; [jitterMs] e o
 * jitter do intervalo inicial e escala com o multiplicador (Spring Framework 7).
 */
@ConfigurationProperties("balance.consumer.backoff")
data class BackOffProperties(
    val initialMs: Long,
    val maxMs: Long,
    val jitterMs: Long,
)

/**
 * Durante a espera do backoff o container fica pausado: o poll continua vivo (sem rebalance por `max.poll.interval.ms`) e a
 * mensagem valida permanece no broker. A retomada e agendada num `TaskScheduler` proprio, nunca por `Thread.sleep` no thread
 * do poll.
 */
@Configuration
@EnableConfigurationProperties(BackOffProperties::class)
class BackpressureConfig {
    /** Um thread basta: cada tarefa so chama `container.resume()`. */
    @Bean
    fun kafkaBackOffScheduler(): ThreadPoolTaskScheduler =
        ThreadPoolTaskScheduler().apply {
            setPoolSize(1)
            setThreadNamePrefix("kafka-backoff-")
            setDaemon(true)
            setRemoveOnCancelPolicy(true)
        }

    /**
     * Recebe o container PAI (`thisOrParentContainer`, Spring Kafka 4.1): a pausa vale para todas as threads de consumo da
     * instancia. E o desejado, pois o armazenamento e compartilhado e todas ficariam falhando.
     */
    @Bean
    fun containerPausingBackOffHandler(kafkaBackOffScheduler: TaskScheduler): BackOffHandler =
        // Sem registro: so se pausa o container recebido do error handler (o pai; a pausa por id exigiria o registro).
        ContainerPausingBackOffHandler(ListenerContainerPauseService(null, kafkaBackOffScheduler))
}
