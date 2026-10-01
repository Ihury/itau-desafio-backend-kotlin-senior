package br.com.itau.challenge.balance.adapter.input.kafka

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("balance.dlt")
class DeadLetterProperties(
    val waitForSendResultTimeout: Duration,
)
