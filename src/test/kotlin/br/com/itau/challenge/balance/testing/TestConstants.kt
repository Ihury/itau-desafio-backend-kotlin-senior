package br.com.itau.challenge.balance.testing

import java.time.Duration
import java.time.ZoneId

val DISPLAY_ZONE: ZoneId = ZoneId.of("America/Sao_Paulo")

val RETRY_AFTER: Duration = Duration.ofSeconds(10)

val RETRY_AFTER_SECONDS: String = RETRY_AFTER.seconds.toString()
