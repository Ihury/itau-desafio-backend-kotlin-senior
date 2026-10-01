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
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class DynamoDbHealthIndicator(
    private val client: DynamoDbClient,
    private val tableName: String,
    meterRegistry: MeterRegistry,
    private val clock: Clock,
) : HealthIndicator {
    private data class ProbeResult(
        val at: Instant,
        val up: Boolean,
    )

    private val lock = ReentrantLock()
    private var lastProbe: ProbeResult? = null

    init {
        Gauge
            .builder(GAUGE_NAME) { if (isUpRefreshingIfStale()) 1.0 else 0.0 }
            .description("1 se o ultimo probe da dependencia foi bem sucedido, 0 se falhou (mesmo estado do grupo de saude dependencies)")
            .tag(DEPENDENCY_TAG, DEPENDENCY)
            .register(meterRegistry)
    }

    override fun health(): Health = if (isUpRefreshingIfStale()) Health.up().build() else Health.down().build()

    private fun isUpRefreshingIfStale(): Boolean =
        lock.withLock {
            val now = clock.instant()
            val previous = lastProbe
            if (previous != null && now.isBefore(previous.at.plus(CACHE_TTL))) return@withLock previous.up
            val failureReason = probeFailureReason()
            val current = ProbeResult(now, up = failureReason == null)
            lastProbe = current
            logTransition(previous?.up, failureReason)
            current.up
        }

    private fun probeFailureReason(): String? =
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
            log.debug("dynamodb probe failed exception={}", failure.javaClass.simpleName)
            failure.javaClass.simpleName
        }

    private fun logTransition(
        previousUp: Boolean?,
        failureReason: String?,
    ) {
        when {
            failureReason != null && previousUp != false -> log.warn("dynamodb dependency is down reason={}", failureReason)
            failureReason == null && previousUp == false -> log.info("dynamodb dependency is up again")
        }
    }

    private companion object {
        private val log = LoggerFactory.getLogger(DynamoDbHealthIndicator::class.java)
        const val GAUGE_NAME = "balance.dependency.up"
        const val DEPENDENCY_TAG = "dependency"
        const val DEPENDENCY = "dynamodb"
        val CACHE_TTL: Duration = Duration.ofSeconds(5)
        val PROBE_TIMEOUT: Duration = Duration.ofMillis(500)
    }
}
