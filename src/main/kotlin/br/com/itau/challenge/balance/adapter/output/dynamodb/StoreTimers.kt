package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.adapter.output.ResultTimers
import io.micrometer.core.instrument.MeterRegistry

internal enum class ReadResult { FOUND, NOT_FOUND, ERROR }

internal enum class WriteResult { APPLIED, CONDITION_FAILED, ERROR }

private val STORE_LATENCY_OBJECTIVES_MILLIS = listOf(5L, 10L, 25L, 50L, 100L, 250L, 500L, 1_000L, 2_000L)

internal fun readTimers(registry: MeterRegistry): ResultTimers<ReadResult> =
    ResultTimers(
        registry = registry,
        name = "balance.store.read.duration",
        description = "Latencia do GetItem do snapshot",
        tagName = "result",
        results = ReadResult.entries,
        objectivesMillis = STORE_LATENCY_OBJECTIVES_MILLIS,
    )

internal fun writeTimers(registry: MeterRegistry): ResultTimers<WriteResult> =
    ResultTimers(
        registry = registry,
        name = "balance.store.write.duration",
        description = "Latencia da UpdateItem condicional do snapshot",
        tagName = "result",
        results = WriteResult.entries,
        objectivesMillis = STORE_LATENCY_OBJECTIVES_MILLIS,
    )
