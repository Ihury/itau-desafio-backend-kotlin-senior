package br.com.itau.challenge.balance.adapter.output.dynamodb

import java.time.Duration

/**
 * Buckets do histograma dos timers `balance.store.read.duration` e `balance.store.write.duration`; o teto de 2 s cobre o
 * timeout de escrita.
 */
internal object StoreLatencyObjectives {
    val OBJECTIVES: Array<Duration> =
        arrayOf(5L, 10L, 25L, 50L, 100L, 250L, 500L, 1_000L, 2_000L).map(Duration::ofMillis).toTypedArray()
}
