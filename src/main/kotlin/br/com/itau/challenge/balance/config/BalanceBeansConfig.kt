package br.com.itau.challenge.balance.config

import br.com.itau.challenge.balance.adapter.output.dynamodb.CircuitBreakingBalanceSnapshotReader
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbBalanceSnapshotReader
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbBalanceSnapshotWriter
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbClientProperties
import br.com.itau.challenge.balance.port.output.BalanceSnapshotReader
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.time.Clock
import java.time.ZoneId

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

    /**
     * Escrita condicional atomica do snapshot, sobre o cliente de ESCRITA (uma tentativa; o retry e do consumer). Nao ha
     * circuit breaker na escrita: o backpressure do consumer cumpre esse papel. As metricas de desfecho
     * (`MicrometerProcessingMetrics`) entram como `@Component` do adapter.
     */
    @Bean
    fun balanceSnapshotWriter(
        @Qualifier("dynamoDbWriteClient") client: DynamoDbClient,
        properties: DynamoDbClientProperties,
    ): BalanceSnapshotWriter = DynamoDbBalanceSnapshotWriter(client, properties.tableName)

    /** Relogio do processamento (usado so para a tolerancia de timestamp futuro; nunca decide precedencia). */
    @Bean
    fun clock(): Clock = Clock.systemUTC()

    /** Fuso de exibicao do offset de `updated_at`. O instante (UTC) e a verdade; o offset e apresentacao. */
    @Bean
    fun displayZone(
        @Value($$"${balance.display-zone}") zone: String,
    ): ZoneId = ZoneId.of(zone)
}
