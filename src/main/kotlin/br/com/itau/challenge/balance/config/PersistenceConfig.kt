package br.com.itau.challenge.balance.config

import br.com.itau.challenge.balance.adapter.output.dynamodb.CircuitBreakingBalanceSnapshotReader
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbBalanceSnapshotReader
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbBalanceSnapshotWriter
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbClientProperties
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbHealthIndicator
import br.com.itau.challenge.balance.port.output.BalanceSnapshotReader
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.time.Clock

@Configuration
class PersistenceConfig {
    @Bean
    fun balanceSnapshotReader(
        @Qualifier("dynamoDbReadClient") client: DynamoDbClient,
        properties: DynamoDbClientProperties,
        meterRegistry: MeterRegistry,
        @Qualifier("dynamoDbReadCircuitBreaker") circuitBreaker: CircuitBreaker,
    ): BalanceSnapshotReader =
        CircuitBreakingBalanceSnapshotReader(
            DynamoDbBalanceSnapshotReader(client, properties.tableName, properties.read.consistent, meterRegistry),
            circuitBreaker,
        )

    @Bean
    fun balanceSnapshotWriter(
        @Qualifier("dynamoDbWriteClient") client: DynamoDbClient,
        properties: DynamoDbClientProperties,
        meterRegistry: MeterRegistry,
    ): BalanceSnapshotWriter = DynamoDbBalanceSnapshotWriter(client, properties.tableName, meterRegistry)

    @Bean
    fun dynamoDbHealthIndicator(
        @Qualifier("dynamoDbReadClient") client: DynamoDbClient,
        properties: DynamoDbClientProperties,
        meterRegistry: MeterRegistry,
        clock: Clock,
    ): DynamoDbHealthIndicator = DynamoDbHealthIndicator(client, properties.tableName, meterRegistry, clock)
}
