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

@ConfigurationProperties("balance.consumer.backoff")
data class BackOffProperties(
    val initialMs: Long,
    val maxMs: Long,
    val jitterMs: Long,
)

@Configuration
@EnableConfigurationProperties(BackOffProperties::class)
class BackpressureConfig {
    @Bean
    fun kafkaBackOffScheduler(): ThreadPoolTaskScheduler =
        ThreadPoolTaskScheduler().apply {
            setPoolSize(1)
            setThreadNamePrefix("kafka-backoff-")
            setDaemon(true)
            setRemoveOnCancelPolicy(true)
        }

    @Bean
    fun containerPausingBackOffHandler(kafkaBackOffScheduler: TaskScheduler): BackOffHandler =
        ContainerPausingBackOffHandler(pauseServiceWithoutRegistry(kafkaBackOffScheduler))

    private fun pauseServiceWithoutRegistry(scheduler: TaskScheduler) = ListenerContainerPauseService(null, scheduler)
}
