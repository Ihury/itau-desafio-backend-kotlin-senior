package br.com.itau.challenge.balance.config

import br.com.itau.challenge.balance.adapter.input.kafka.TransactionEventParser
import br.com.itau.challenge.balance.application.FutureTolerance
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@EnableConfigurationProperties(BalanceProperties::class)
class EventParsingConfig {
    @Bean
    fun transactionEventParser(properties: BalanceProperties): TransactionEventParser =
        TransactionEventParser(properties.minEventTimestamp, properties.minAccountCreatedAt)

    @Bean
    fun futureTolerance(properties: BalanceProperties): FutureTolerance = FutureTolerance(properties.futureTolerance)
}
