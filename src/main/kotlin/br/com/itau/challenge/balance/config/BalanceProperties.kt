package br.com.itau.challenge.balance.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration
import java.time.Instant
import java.time.ZoneId

@ConfigurationProperties("balance")
class BalanceProperties(
    val minEventTimestamp: Instant,
    val minAccountCreatedAt: Instant,
    val futureTolerance: Duration,
    val displayZone: ZoneId,
)
