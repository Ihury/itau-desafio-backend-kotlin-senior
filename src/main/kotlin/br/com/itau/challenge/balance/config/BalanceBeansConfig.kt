package br.com.itau.challenge.balance.config

import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbBalanceSnapshotReader
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbClientProperties
import br.com.itau.challenge.balance.port.output.BalanceSnapshotReader
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
    @Bean
    fun balanceSnapshotReader(
        @Qualifier("dynamoDbReadClient") client: DynamoDbClient,
        properties: DynamoDbClientProperties,
        meterRegistry: MeterRegistry,
    ): BalanceSnapshotReader =
        DynamoDbBalanceSnapshotReader(client, properties.tableName, properties.read.consistent, meterRegistry)
}
