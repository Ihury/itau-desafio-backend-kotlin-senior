package br.com.itau.challenge.balance.config

import org.junit.jupiter.api.Test
import org.springframework.boot.context.properties.bind.BindException
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class BalancePropertiesTest {
    private fun bind(overrides: Map<String, String> = emptyMap()): BalanceProperties {
        val source =
            MapConfigurationPropertySource(
                mapOf(
                    "balance.min-event-timestamp" to "2000-01-01T00:00:00Z",
                    "balance.min-account-created-at" to "1900-01-01T00:00:00Z",
                    "balance.future-tolerance" to "PT5M",
                    "balance.display-zone" to "America/Sao_Paulo",
                ) + overrides,
            )
        return Binder(source).bind("balance", BalanceProperties::class.java).get()
    }

    @Test
    fun `the documented values are bound to typed properties`() {
        val properties = bind()

        assertEquals(Instant.parse("2000-01-01T00:00:00Z"), properties.minEventTimestamp)
        assertEquals(Instant.parse("1900-01-01T00:00:00Z"), properties.minAccountCreatedAt)
        assertEquals(Duration.ofMinutes(5), properties.futureTolerance)
        assertEquals(ZoneId.of("America/Sao_Paulo"), properties.displayZone)
    }

    @Test
    fun `an invalid value fails the binding`() {
        assertFailsWith<BindException> { bind(mapOf("balance.display-zone" to "Nao/Existe")) }
        assertFailsWith<BindException> { bind(mapOf("balance.min-event-timestamp" to "ontem")) }
        assertFailsWith<BindException> { bind(mapOf("balance.future-tolerance" to "cinco minutos")) }
    }
}
