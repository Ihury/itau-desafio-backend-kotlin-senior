package br.com.itau.challenge.balance.testing

import java.time.Instant

private const val MICROS_PER_SECOND = 1_000_000L
private const val NANOS_PER_MICRO = 1_000L

fun Instant.toEpochMicros(): Long = epochSecond * MICROS_PER_SECOND + nano / NANOS_PER_MICRO
