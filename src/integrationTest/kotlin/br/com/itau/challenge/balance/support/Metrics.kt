package br.com.itau.challenge.balance.support

import io.micrometer.core.instrument.MeterRegistry

fun MeterRegistry.eventCount(
    outcome: String,
    reason: String = "none",
): Double = get("balance.events").tags("outcome", outcome, "reason", reason).counter().count()

fun MeterRegistry.processedCount(): Double = eventCount("processed")

fun MeterRegistry.rejectedCount(reason: String): Double = eventCount("rejected", reason)

fun MeterRegistry.backpressureCount(cause: String): Double = get("balance.consumer.backpressure").tag("cause", cause).counter().count()

fun MeterRegistry.backpressureTotal(): Double = find("balance.consumer.backpressure").counters().sumOf { it.count() }
