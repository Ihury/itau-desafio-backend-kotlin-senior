package br.com.itau.challenge.balance.adapter.output

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.time.Duration
import java.util.concurrent.TimeUnit

internal class ResultTimers<R : Enum<R>>(
    registry: MeterRegistry,
    name: String,
    description: String,
    tagName: String,
    results: Collection<R>,
    objectivesMillis: List<Long>,
) {
    private val timers: Map<R, Timer> =
        results.associateWith { result ->
            Timer
                .builder(name)
                .description(description)
                .tag(tagName, result.name.lowercase())
                .serviceLevelObjectives(*objectivesMillis.map(Duration::ofMillis).toTypedArray())
                .register(registry)
        }

    fun startNanos(): Long = System.nanoTime()

    fun record(
        result: R,
        startedNanos: Long,
    ) = recordElapsed(result, System.nanoTime() - startedNanos)

    fun recordElapsed(
        result: R,
        nanos: Long,
    ) = timers.getValue(result).record(nanos, TimeUnit.NANOSECONDS)
}
