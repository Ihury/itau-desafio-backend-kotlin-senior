package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.AccountStatus
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.TransactionEvent
import br.com.itau.challenge.balance.domain.model.TransactionStatus
import br.com.itau.challenge.balance.domain.model.TransactionType
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TransactionEventParserTest {
    private val parser =
        TransactionEventParser(
            minEventTimestamp = Instant.parse("2000-01-01T00:00:00Z"),
            minAccountCreatedAt = Instant.parse("1900-01-01T00:00:00Z"),
        )

    /** Os 12 campos obrigatorios, na ordem documentada, com valores validos (JSON literal). */
    private val validFields: LinkedHashMap<String, String> =
        linkedMapOf(
            "transaction.id" to "\"8e8ae808-b154-48b5-9f3e-553935cc4543\"",
            "transaction.type" to "\"CREDIT\"",
            "transaction.amount" to "97.07",
            "transaction.currency" to "\"BRL\"",
            "transaction.status" to "\"APPROVED\"",
            "transaction.timestamp" to "1751641364589998",
            "account.id" to "\"5b19c8b6-0cc4-4c72-a989-0c2ee15fa975\"",
            "account.owner" to "\"315e3cfe-f4af-4cd2-b298-a449e614349a\"",
            "account.created_at" to "1634874339000000",
            "account.status" to "\"ENABLED\"",
            "account.balance.amount" to "183.12",
            "account.balance.currency" to "\"BRL\"",
        )

    /** Monta o JSON; valor `null` (Kotlin) omite a chave; o literal `null` gera JSON null. */
    private fun payloadJson(
        overrides: Map<String, String?> = emptyMap(),
        extras: String = "",
    ): String {
        val fields = validFields.toMutableMap<String, String?>().apply { putAll(overrides) }

        fun field(path: String): String? = fields[path]?.let { "\"${path.substringAfterLast('.')}\":$it" }

        fun obj(vararg paths: String): String = paths.mapNotNull { field(it) }.joinToString(",")
        val transaction = obj("transaction.id", "transaction.type", "transaction.amount", "transaction.currency", "transaction.status", "transaction.timestamp")
        val balance = obj("account.balance.amount", "account.balance.currency")
        val account = obj("account.id", "account.owner", "account.created_at", "account.status")
        return "{\"transaction\":{$transaction$extras},\"account\":{$account,\"balance\":{$balance}}}"
    }

    private fun parse(text: String): TransactionEvent = parser.parse(text.toByteArray(Charsets.UTF_8))

    private fun rejection(bytes: ByteArray?): InvalidEventException = assertFailsWith<InvalidEventException> { parser.parse(bytes) }

    private fun rejection(text: String): InvalidEventException = rejection(text.toByteArray(Charsets.UTF_8))

    private fun assertRejected(
        expected: RejectionReason,
        text: String,
        detail: String? = null,
        label: String = text.take(80),
    ) {
        val failure = rejection(text)
        assertEquals(expected, failure.reason, label)
        assertEquals(detail, failure.fieldPath, "detail de $label")
    }

    @Test
    fun `the example of the schema is parsed exactly`() {
        val event = parse(payloadJson())

        assertEquals("8e8ae808-b154-48b5-9f3e-553935cc4543", event.transaction.id.value)
        assertEquals(TransactionType.CREDIT, event.transaction.type)
        assertEquals(BigDecimal("97.07"), event.transaction.amount)
        assertEquals("BRL", event.transaction.currency.value)
        assertEquals(TransactionStatus.APPROVED, event.transaction.status)
        assertEquals(1751641364589998L, event.transaction.timestamp.micros)
        assertEquals("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975", event.account.id.value)
        assertEquals("315e3cfe-f4af-4cd2-b298-a449e614349a", event.account.owner.value)
        assertEquals(1634874339000000L, event.account.createdAt.micros)
        assertEquals(AccountStatus.ENABLED, event.account.status)
        assertEquals(BigDecimal("183.12"), event.account.balance.amount)
        assertEquals("BRL", event.account.balance.currency.value)
    }

    @Test
    fun `decimals never go through double`() {
        val event = parse(payloadJson(mapOf("transaction.amount" to "0.1", "account.balance.amount" to "12345678901234567890.123456789012345678")))

        assertEquals(BigDecimal("0.1"), event.transaction.amount)
        assertEquals(BigDecimal("12345678901234567890.123456789012345678"), event.account.balance.amount)
    }

    @Test
    fun `uppercase identifiers are normalized to lower case`() {
        val event =
            parse(
                payloadJson(
                    mapOf(
                        "transaction.id" to "\"8E8AE808-B154-48B5-9F3E-553935CC4543\"",
                        "account.id" to "\"5B19C8B6-0CC4-4C72-A989-0C2EE15FA975\"",
                        "account.owner" to "\"315E3CFE-F4AF-4CD2-B298-A449E614349A\"",
                    ),
                ),
            )

        assertEquals("8e8ae808-b154-48b5-9f3e-553935cc4543", event.transaction.id.value)
        assertEquals("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975", event.account.id.value)
        assertEquals("315e3cfe-f4af-4cd2-b298-a449e614349a", event.account.owner.value)
    }

    @Test
    fun `unknown extra fields are ignored at every level`() {
        val withExtras = payloadJson(extras = ",\"note\":{\"a\":[1,2,3]}").replace("{\"transaction\"", "{\"trace\":\"x\",\"transaction\"")

        assertEquals(parse(payloadJson()), parse(withExtras))
    }

    @Test
    fun `a positive exponent is expanded to scale zero`() {
        val event = parse(payloadJson(mapOf("account.balance.amount" to "1E+3", "transaction.amount" to "2E+2")))

        assertEquals(BigDecimal("1000"), event.account.balance.amount)
        assertEquals(0, event.account.balance.amount.scale())
        assertEquals(BigDecimal("200"), event.transaction.amount)
    }

    @Test
    fun `zero and negative balances are valid and a zero transaction amount is valid`() {
        assertEquals(BigDecimal("-42.50"), parse(payloadJson(mapOf("account.balance.amount" to "-42.50"))).account.balance.amount)
        assertEquals(0, parse(payloadJson(mapOf("account.balance.amount" to "0", "transaction.amount" to "0"))).account.balance.amount.signum())
    }

    @Test
    fun `an account created before 2000 is valid`() {
        val micros = Instant.parse("1998-06-01T00:00:00Z").let { it.epochSecond * 1_000_000 }

        assertEquals(micros, parse(payloadJson(mapOf("account.created_at" to micros.toString()))).account.createdAt.micros)
    }

    @Test
    fun `a payload of exactly 64 KiB is accepted`() {
        val base = payloadJson()
        val padded = base + " ".repeat(64 * 1024 - base.length)

        assertEquals(64 * 1024, padded.length)
        assertEquals(parse(base), parse(padded))
    }

    @Test
    fun `null and empty payloads are malformed`() {
        assertEquals(RejectionReason.MALFORMED_PAYLOAD, rejection(null as ByteArray?).reason)
        assertEquals(RejectionReason.MALFORMED_PAYLOAD, rejection(ByteArray(0)).reason)
        assertEquals(RejectionReason.MALFORMED_PAYLOAD, rejection("   ").reason)
    }

    @Test
    fun `a payload one byte over 64 KiB is malformed`() {
        val base = payloadJson()
        val padded = base + " ".repeat(64 * 1024 + 1 - base.length)

        assertEquals(64 * 1024 + 1, padded.length)
        assertEquals(RejectionReason.MALFORMED_PAYLOAD, rejection(padded).reason)
    }

    @Test
    fun `invalid utf8 is malformed anywhere in the payload`() {
        val alone = byteArrayOf(0xC3.toByte(), 0x28)
        val insideString = "{\"a\":\"".toByteArray() + alone + "\"}".toByteArray()

        assertEquals(RejectionReason.MALFORMED_PAYLOAD, rejection(alone).reason)
        assertEquals(RejectionReason.MALFORMED_PAYLOAD, rejection(insideString).reason)
        assertNull(rejection(insideString).fieldPath)
    }

    @Test
    fun `broken json shapes are malformed with no detail`() {
        val dup = "{\"transaction\":{\"id\":\"a\"},\"transaction\":{\"id\":\"b\"},\"account\":{}}"
        val deep = "{\"x\":" + "[".repeat(500) + "]".repeat(500) + "}" // profundidade total 501
        val shapes =
            mapOf(
                "not json" to "{not json",
                "duplicate key" to dup,
                "duplicate key deep" to payloadJson().replace("\"currency\":\"BRL\"", "\"currency\":\"BRL\",\"currency\":\"USD\""),
                "tokens after the document" to payloadJson() + "{}",
                "trailing garbage" to payloadJson() + "x",
                "NaN" to payloadJson(mapOf("transaction.amount" to "NaN")),
                "depth 501" to deep,
                "number with 1001 chars" to payloadJson(mapOf("transaction.amount" to "1".repeat(1001))),
                "root array" to "[]",
                "root string" to "\"x\"",
                "root null" to "null",
                "root number" to "42",
                "transaction array" to payloadJson().replace(Regex("\"transaction\":\\{[^}]*}"), "\"transaction\":[]"),
                "transaction string" to payloadJson().replace(Regex("\"transaction\":\\{[^}]*}"), "\"transaction\":\"x\""),
                "transaction number" to payloadJson().replace(Regex("\"transaction\":\\{[^}]*}"), "\"transaction\":1"),
                "account string" to "{\"transaction\":{},\"account\":\"x\"}",
                "balance string" to payloadJson().replace(Regex("\"balance\":\\{[^}]*}"), "\"balance\":\"x\""),
                "balance array" to payloadJson().replace(Regex("\"balance\":\\{[^}]*}"), "\"balance\":[1]"),
            )
        shapes.forEach { (label, text) -> assertRejected(RejectionReason.MALFORMED_PAYLOAD, text, detail = null, label = label) }
    }

    @Test
    fun `a nesting depth of exactly 500 is not rejected for depth`() {
        val nested = "[".repeat(499) + "]".repeat(499) // profundidade total 500

        // Profundidade tolerada: o documento e valido e o unico problema passa a ser o campo obrigatorio ausente.
        assertRejected(RejectionReason.MISSING_FIELD, "{\"x\":$nested}", detail = "transaction")
    }

    @Test
    fun `a one thousand digit number is accepted by the syntax and rejected as a value`() {
        assertRejected(RejectionReason.INVALID_VALUE, payloadJson(mapOf("transaction.amount" to "1".repeat(1000))), "transaction.amount")
    }

    @Test
    fun `a structure that is not an object beats a missing field`() {
        assertRejected(RejectionReason.MALFORMED_PAYLOAD, payloadJson(mapOf("account.owner" to null)).replace(Regex("\"balance\":\\{[^}]*}"), "\"balance\":\"x\""))
    }

    @Test
    fun `each of the 12 fields absent or null is a missing field carrying its path`() {
        assertEquals(12, validFields.size)
        validFields.keys.forEach { path ->
            assertRejected(RejectionReason.MISSING_FIELD, payloadJson(mapOf(path to null)), path, "$path ausente")
            assertRejected(RejectionReason.MISSING_FIELD, payloadJson(mapOf(path to "null")), path, "$path null")
        }
    }

    @Test
    fun `absent or null parent objects are missing fields at the parent path`() {
        assertRejected(RejectionReason.MISSING_FIELD, "{\"account\":{}}", "transaction")
        assertRejected(RejectionReason.MISSING_FIELD, "{\"transaction\":null,\"account\":null}", "transaction")
        assertRejected(RejectionReason.MISSING_FIELD, payloadJson().replace(Regex(",\"balance\":\\{[^}]*}"), ""), "account.balance")
        assertRejected(RejectionReason.MISSING_FIELD, payloadJson().replace(Regex("\"balance\":\\{[^}]*}"), "\"balance\":null"), "account.balance")
        assertRejected(RejectionReason.MISSING_FIELD, "{\"transaction\":{}}", "transaction.id")
    }

    @Test
    fun `identifiers that are not canonical uuids are invalid identifiers`() {
        listOf("transaction.id", "account.id", "account.owner").forEach { path ->
            listOf("\"1-1-1-1-1\"", "123", "\"\"", "\"not-a-uuid\"", "true", "{}", "[]", "\" 8e8ae808-b154-48b5-9f3e-553935cc4543\"").forEach { bad ->
                assertRejected(RejectionReason.INVALID_IDENTIFIER, payloadJson(mapOf(path to bad)), path, "$path=$bad")
            }
        }
    }

    @Test
    fun `amounts that are not valid numbers are invalid values`() {
        val bad = listOf("\"10.00\"", "true", "{}", "[]", "-0.01", "-1", "123456789012345678901234567890123456789", "0.000000000000000000000000000000000000001", "1E999999999", "1E39")
        listOf("transaction.amount", "account.balance.amount").forEach { path ->
            bad.forEach { value ->
                if (path == "account.balance.amount" && value.startsWith("-")) return@forEach // saldo negativo e valido
                assertRejected(RejectionReason.INVALID_VALUE, payloadJson(mapOf(path to value)), path, "$path=$value")
            }
        }
    }

    @Test
    fun `a huge exponent is rejected without being expanded`() {
        val started = System.nanoTime()

        assertRejected(RejectionReason.INVALID_VALUE, payloadJson(mapOf("account.balance.amount" to "1E999999999")), "account.balance.amount")

        assertTrue((System.nanoTime() - started) < 2_000_000_000L, "a rejeicao nao pode materializar o expoente")
    }

    @Test
    fun `thirty eight digits are accepted and thirty nine are not`() {
        parse(payloadJson(mapOf("account.balance.amount" to "12345678901234567890123456789012345678")))

        assertRejected(RejectionReason.INVALID_VALUE, payloadJson(mapOf("account.balance.amount" to "123456789012345678901234567890123456789")), "account.balance.amount")
    }

    @Test
    fun `currencies that are not iso 4217 in upper case are invalid currencies`() {
        listOf("transaction.currency", "account.balance.currency").forEach { path ->
            listOf("\"brl\"", "\"BR\"", "\"BRLL\"", "\"ZZZ\"", "5", "true", "\"\"").forEach { bad ->
                assertRejected(RejectionReason.INVALID_CURRENCY, payloadJson(mapOf(path to bad)), path, "$path=$bad")
            }
        }
    }

    @Test
    fun `domain values outside the enumerations are unknown domain values`() {
        mapOf(
            "transaction.type" to listOf("\"TRANSFER\"", "\"credit\"", "\"Credit\"", "1"),
            "transaction.status" to listOf("\"PENDING\"", "\"approved\"", "\"\""),
            "account.status" to listOf("\"SUSPENDED\"", "\"enabled\"", "\"Enabled\""),
        ).forEach { (path, values) ->
            values.forEach { bad -> assertRejected(RejectionReason.UNKNOWN_DOMAIN_VALUE, payloadJson(mapOf(path to bad)), path, "$path=$bad") }
        }
    }

    @Test
    fun `timestamps that are not plausible integer microseconds are invalid timestamps`() {
        val year1999 = Instant.parse("1999-12-31T23:59:59Z").epochSecond * 1_000_000
        val year1850 = Instant.parse("1850-01-01T00:00:00Z").epochSecond * 1_000_000
        val transactionCases =
            listOf(
                "1751641364589998.0",
                "1.75E15",
                "\"1751641364589998\"",
                "true",
                "99999999999999999999",
                "1751641364589",
                "1751641364",
                year1999.toString(),
                "-1",
            )
        transactionCases.forEach { bad ->
            assertRejected(RejectionReason.INVALID_TIMESTAMP, payloadJson(mapOf("transaction.timestamp" to bad)), "transaction.timestamp", "transaction.timestamp=$bad")
        }
        listOf("1634874339000000.0", "\"1634874339000000\"", "99999999999999999999", year1850.toString()).forEach { bad ->
            assertRejected(RejectionReason.INVALID_TIMESTAMP, payloadJson(mapOf("account.created_at" to bad)), "account.created_at", "account.created_at=$bad")
        }
    }

    @Test
    fun `the minimum timestamps come from the injected configuration`() {
        val strict = TransactionEventParser(Instant.parse("2025-01-01T00:00:00Z"), Instant.parse("2020-01-01T00:00:00Z"))
        val bytes = payloadJson().toByteArray()

        assertEquals(RejectionReason.INVALID_TIMESTAMP, assertFailsWith<InvalidEventException> { strict.parse(payloadJson(mapOf("transaction.timestamp" to "1700000000000000")).toByteArray()) }.reason)
        assertEquals(RejectionReason.INVALID_TIMESTAMP, assertFailsWith<InvalidEventException> { strict.parse(payloadJson(mapOf("account.created_at" to "1500000000000000")).toByteArray()) }.reason)
        strict.parse(bytes)
    }

    @Test
    fun `the first invalid field in the documented order decides the reason`() {
        assertRejected(
            RejectionReason.INVALID_IDENTIFIER,
            payloadJson(mapOf("transaction.id" to "\"1-1-1-1-1\"", "transaction.currency" to "\"brl\"")),
            "transaction.id",
        )
        assertRejected(
            RejectionReason.INVALID_VALUE,
            payloadJson(mapOf("transaction.amount" to "-1", "transaction.currency" to "\"brl\"", "transaction.status" to "\"PENDING\"")),
            "transaction.amount",
        )
        assertRejected(
            RejectionReason.INVALID_CURRENCY,
            payloadJson(mapOf("transaction.currency" to "\"brl\"", "transaction.status" to "\"PENDING\"", "transaction.timestamp" to "1")),
            "transaction.currency",
        )
        assertRejected(
            RejectionReason.UNKNOWN_DOMAIN_VALUE,
            payloadJson(mapOf("transaction.type" to "\"TRANSFER\"", "transaction.amount" to "-1")),
            "transaction.type",
        )
        assertRejected(
            RejectionReason.INVALID_TIMESTAMP,
            payloadJson(mapOf("transaction.timestamp" to "1", "account.id" to "\"x\"")),
            "transaction.timestamp",
        )
        assertRejected(
            RejectionReason.INVALID_VALUE,
            payloadJson(mapOf("account.balance.amount" to "\"1\"", "account.balance.currency" to "\"brl\"")),
            "account.balance.amount",
        )
    }

    @Test
    fun `a missing field beats an invalid one`() {
        assertRejected(
            RejectionReason.MISSING_FIELD,
            payloadJson(mapOf("transaction.id" to "\"1-1-1-1-1\"", "account.balance.currency" to null)),
            "account.balance.currency",
        )
    }

    @Test
    fun `neither the message nor the detail nor the cause carry values of the payload`() {
        val secret = "SEGREDO-123"
        val payloads =
            listOf(
                payloadJson(mapOf("transaction.currency" to "\"$secret\"")),
                payloadJson(mapOf("transaction.id" to "\"$secret\"")),
                payloadJson(mapOf("transaction.type" to "\"$secret\"")),
                payloadJson(mapOf("transaction.amount" to "\"$secret\"")),
                payloadJson(mapOf("transaction.timestamp" to "\"$secret\"")),
                "{not json $secret",
                "{\"a\":\"$secret\",\"a\":1}",
                "[\"$secret\"]",
            )
        payloads.forEach { payload ->
            val failure = rejection(payload)

            assertTrue(secret !in failure.message.orEmpty(), "message de $payload")
            assertTrue(secret !in failure.fieldPath.orEmpty(), "detail de $payload")
            assertTrue(secret !in failure.toString(), "toString de $payload")
            assertNull(failure.cause, "cause de $payload")
            assertEquals(failure.reason.code, failure.message)
        }
    }

    @Test
    fun `a valid payload with a secret in an ignored field does not fail`() {
        parse(payloadJson(extras = ",\"note\":\"SEGREDO-123\""))
    }
}
