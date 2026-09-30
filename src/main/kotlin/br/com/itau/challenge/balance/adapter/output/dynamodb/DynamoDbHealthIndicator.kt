package br.com.itau.challenge.balance.adapter.output.dynamodb

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.boot.health.contributor.Health
import org.springframework.boot.health.contributor.HealthIndicator
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.TableStatus
import java.time.Clock
import java.time.Duration
import java.time.Instant

/**
 * Saude da dependencia DynamoDB (contracts/observability.md, FR-033): probe `DescribeTable` da tabela do snapshot, com timeout
 * curto ([PROBE_TIMEOUT], por cima dos timeouts do cliente de leitura), resultado em cache por [CACHE_TTL] (o probe e barato, mas
 * a saude e consultada por operadores e por raspagens de metricas) e SEM detalhes: nem mensagem de excecao nem nome de
 * infraestrutura chegam a resposta ou aos logs (so a classe da excecao, na transicao de estado).
 *
 * Pertence ao grupo `dependencies`, NUNCA a `liveness` nem a `readiness`: com o DynamoDB fora a instancia continua em rotacao e a
 * API responde 503 + `Retry-After` de forma explicita (Constitution V e VII).
 *
 * O gauge `balance.dependency.up{dependency=dynamodb}` (1 = ultimo probe OK, 0 = falhou) le o MESMO estado em cache: avaliar o
 * gauge pode disparar o probe, entao o estado e renovado a cada raspagem mesmo que ninguem consulte `/actuator/health/dependencies`.
 * "Tabela utilizavel" e `ACTIVE` ou `UPDATING`; qualquer outro estado ou qualquer excecao e `DOWN`.
 */
class DynamoDbHealthIndicator(
    private val client: DynamoDbClient,
    private val tableName: String,
    meterRegistry: MeterRegistry,
    private val clock: Clock,
) : HealthIndicator {
    private val lock = Any()
    private var probedAt: Instant? = null
    private var up: Boolean = false

    init {
        Gauge
            .builder("balance.dependency.up") { if (isUp()) 1.0 else 0.0 }
            .description("1 se o ultimo probe da dependencia foi bem sucedido, 0 se falhou (mesmo estado do grupo de saude dependencies)")
            .tag("dependency", "dynamodb")
            .register(meterRegistry)
    }

    override fun health(): Health = if (isUp()) Health.up().build() else Health.down().build()

    /** Estado em cache; um unico thread executa o probe e os demais reaproveitam o resultado. */
    private fun isUp(): Boolean =
        synchronized(lock) {
            val now = clock.instant()
            val last = probedAt
            if (last == null || !now.isBefore(last.plus(CACHE_TTL))) {
                val previous = if (last == null) null else up
                val failure = probe()
                up = failure == null
                probedAt = now
                logTransition(previous, failure)
            }
            up
        }

    /** `null` quando a tabela esta utilizavel; caso contrario o motivo (classe da excecao ou estado da tabela) para o log. */
    private fun probe(): String? =
        try {
            val request =
                DescribeTableRequest
                    .builder()
                    .tableName(tableName)
                    .overrideConfiguration { it.apiCallTimeout(PROBE_TIMEOUT).apiCallAttemptTimeout(PROBE_TIMEOUT) }
                    .build()
            val status = client.describeTable(request).table()?.tableStatus()
            if (status == TableStatus.ACTIVE || status == TableStatus.UPDATING) null else "table status ${status ?: "unknown"}"
        } catch (failure: RuntimeException) {
            failure.javaClass.simpleName
        }

    private fun logTransition(
        previous: Boolean?,
        failure: String?,
    ) {
        when {
            failure != null && previous != false -> log.warn("dynamodb dependency is down reason={}", failure)
            failure == null && previous == false -> log.info("dynamodb dependency is up again")
        }
    }

    private companion object {
        private val log = LoggerFactory.getLogger(DynamoDbHealthIndicator::class.java)
        val CACHE_TTL: Duration = Duration.ofSeconds(5)
        val PROBE_TIMEOUT: Duration = Duration.ofMillis(500)
    }
}
