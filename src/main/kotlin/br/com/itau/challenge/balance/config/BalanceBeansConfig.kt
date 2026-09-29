package br.com.itau.challenge.balance.config

import br.com.itau.challenge.balance.adapter.output.dynamodb.CircuitBreakingBalanceSnapshotReader
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbBalanceSnapshotReader
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbClientProperties
import br.com.itau.challenge.balance.port.output.BalanceSnapshotReader
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

/**
 * Composition root do contexto `balance`: liga as portas de saida aos adapters. Nada fora de `config` pode importar
 * este pacote.
 */
@Configuration
class BalanceBeansConfig {
    /**
     * Leitura decorada com circuit breaker. O leitor cru (DynamoDB) nao e um bean: so existe embrulhado, para que nenhum
     * consumidor da porta contorne a falha rapida.
     */
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
}
