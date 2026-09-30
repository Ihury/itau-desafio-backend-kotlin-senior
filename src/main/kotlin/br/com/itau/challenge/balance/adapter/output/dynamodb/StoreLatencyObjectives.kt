package br.com.itau.challenge.balance.adapter.output.dynamodb

import java.time.Duration

/**
 * Objetivos de nivel de servico (buckets do histograma) dos timers `balance.store.read.duration` e
 * `balance.store.write.duration` (contracts/observability.md): de 5 ms ate 2 s, cobrindo o timeout de escrita (2 s) e o de leitura.
 */
internal object StoreLatencyObjectives {
    val DURATIONS: Array<Duration> =
        arrayOf(5L, 10L, 25L, 50L, 100L, 250L, 500L, 1_000L, 2_000L).map(Duration::ofMillis).toTypedArray()
}
