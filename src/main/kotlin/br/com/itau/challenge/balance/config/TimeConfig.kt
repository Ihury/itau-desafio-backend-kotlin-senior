package br.com.itau.challenge.balance.config

import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock
import java.time.ZoneId

@Configuration
@EnableConfigurationProperties(BalanceProperties::class)
class TimeConfig {
    @Bean
    fun clock(): Clock = Clock.systemUTC()

    @Bean
    fun displayZone(properties: BalanceProperties): ZoneId = properties.displayZone
}
