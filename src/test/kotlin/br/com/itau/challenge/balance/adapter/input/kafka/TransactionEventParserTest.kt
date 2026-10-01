package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.AccountStatus
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.TransactionEvent
import br.com.itau.challenge.balance.domain.model.TransactionStatus
import br.com.itau.challenge.balance.domain.model.TransactionType
import br.com.itau.challenge.balance.testing.TransactionPayloads.ABSENT_FIELD
import br.com.itau.challenge.balance.testing.TransactionPayloads.JSON_NULL
import br.com.itau.challenge.balance.testing.TransactionPayloads.json as payloadJson
import br.com.itau.challenge.balance.testing.TransactionPayloads.quoted
import br.com.itau.challenge.balance.testing.TransactionPayloads.validFieldsInDocumentedOrder
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import java.math.BigDecimal
import java.time.Instant
import java.util.stream.Stream
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
                        "transaction.id" to quoted("8E8AE808-B154-48B5-9F3E-553935CC4543"),
                        "account.id" to quoted("5B19C8B6-0CC4-4C72-A989-0C2EE15FA975"),
                        "account.owner" to quoted("315E3CFE-F4AF-4CD2-B298-A449E614349A"),
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
    fun `a negative balance is valid`() {
        assertEquals(BigDecimal("-42.50"), parse(payloadJson(mapOf("account.balance.amount" to "-42.50"))).account.balance.amount)
    }

    @Test
    fun `a zero balance and a zero transaction amount are valid`() {
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

    @ParameterizedTest(name = "{0}")
    @MethodSource("brokenShapes")
    fun `broken json shapes are malformed with no detail`(
        label: String,
        text: String,
    ) {
        assertRejected(RejectionReason.MALFORMED_PAYLOAD, text, detail = null, label = label)
    }

    @Test
    fun `a nesting depth of exactly 500 is not rejected for depth, so only the missing required field is reported`() {
        assertRejected(RejectionReason.MISSING_FIELD, documentNestedTo(totalDepth = 500), detail = "transaction")
    }

    @Test
    fun `a one thousand digit number is accepted by the syntax and rejected as a value`() {
        assertRejected(RejectionReason.INVALID_VALUE, payloadJson(mapOf("transaction.amount" to "1".repeat(1000))), "transaction.amount")
    }

    @Test
    fun `a structure that is not an object beats a missing field`() {
        assertRejected(RejectionReason.MALFORMED_PAYLOAD, payloadJson(mapOf("account.owner" to ABSENT_FIELD)).replace(Regex("\"balance\":\\{[^}]*}"), "\"balance\":\"x\""))
    }

    @Test
    fun `each of the 12 fields absent or null is a missing field carrying its path`() {
        assertEquals(12, validFieldsInDocumentedOrder.size)
        validFieldsInDocumentedOrder.keys.forEach { path ->
            assertRejected(RejectionReason.MISSING_FIELD, payloadJson(mapOf(path to ABSENT_FIELD)), path, "$path ausente")
            assertRejected(RejectionReason.MISSING_FIELD, payloadJson(mapOf(path to JSON_NULL)), path, "$path null")
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

    @ParameterizedTest
    @ValueSource(strings = ["transaction.id", "account.id", "account.owner"])
    fun `identifiers that are not canonical uuids are invalid identifiers`(path: String) {
        INVALID_IDENTIFIERS.forEach { bad ->
            assertRejected(RejectionReason.INVALID_IDENTIFIER, payloadJson(mapOf(path to bad)), path, "$path=$bad")
        }
    }

    @Test
    fun `a transaction amount that is not a valid non negative number is an invalid value`() {
        (INVALID_AMOUNTS + NEGATIVE_AMOUNTS).forEach { value ->
            assertRejected(RejectionReason.INVALID_VALUE, payloadJson(mapOf("transaction.amount" to value)), "transaction.amount", "transaction.amount=$value")
        }
    }

    @Test
    fun `a balance amount that is not a valid number is an invalid value`() {
        INVALID_AMOUNTS.forEach { value ->
            assertRejected(RejectionReason.INVALID_VALUE, payloadJson(mapOf("account.balance.amount" to value)), "account.balance.amount", "account.balance.amount=$value")
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

    @ParameterizedTest
    @ValueSource(strings = ["transaction.currency", "account.balance.currency"])
    fun `currencies that are not iso 4217 in upper case are invalid currencies`(path: String) {
        listOf(quoted("brl"), quoted("BR"), quoted("BRLL"), quoted("ZZZ"), "5", "true", quoted("")).forEach { bad ->
            assertRejected(RejectionReason.INVALID_CURRENCY, payloadJson(mapOf(path to bad)), path, "$path=$bad")
        }
    }

    @Test
    fun `domain values outside the enumerations are unknown domain values`() {
        mapOf(
            "transaction.type" to listOf(quoted("TRANSFER"), quoted("credit"), quoted("Credit"), "1"),
            "transaction.status" to listOf(quoted("PENDING"), quoted("approved"), quoted("")),
            "account.status" to listOf(quoted("SUSPENDED"), quoted("enabled"), quoted("Enabled")),
        ).forEach { (path, values) ->
            values.forEach { bad -> assertRejected(RejectionReason.UNKNOWN_DOMAIN_VALUE, payloadJson(mapOf(path to bad)), path, "$path=$bad") }
        }
    }

    @Test
    fun `transaction timestamps that are not plausible integer microseconds are invalid timestamps`() {
        val year1999 = Instant.parse("1999-12-31T23:59:59Z").epochSecond * 1_000_000
        val transactionCases =
            listOf(
                "1751641364589998.0",
                "1.75E15",
                quoted("1751641364589998"),
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
    }

    @Test
    fun `account creation timestamps that are not plausible integer microseconds are invalid timestamps`() {
        val year1850 = Instant.parse("1850-01-01T00:00:00Z").epochSecond * 1_000_000
        listOf("1634874339000000.0", quoted("1634874339000000"), "99999999999999999999", year1850.toString()).forEach { bad ->
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
            payloadJson(mapOf("transaction.id" to quoted("1-1-1-1-1"), "transaction.currency" to quoted("brl"))),
            "transaction.id",
        )
        assertRejected(
            RejectionReason.INVALID_VALUE,
            payloadJson(mapOf("transaction.amount" to "-1", "transaction.currency" to quoted("brl"), "transaction.status" to quoted("PENDING"))),
            "transaction.amount",
        )
        assertRejected(
            RejectionReason.INVALID_CURRENCY,
            payloadJson(mapOf("transaction.currency" to quoted("brl"), "transaction.status" to quoted("PENDING"), "transaction.timestamp" to "1")),
            "transaction.currency",
        )
        assertRejected(
            RejectionReason.UNKNOWN_DOMAIN_VALUE,
            payloadJson(mapOf("transaction.type" to quoted("TRANSFER"), "transaction.amount" to "-1")),
            "transaction.type",
        )
        assertRejected(
            RejectionReason.INVALID_TIMESTAMP,
            payloadJson(mapOf("transaction.timestamp" to "1", "account.id" to quoted("x"))),
            "transaction.timestamp",
        )
        assertRejected(
            RejectionReason.INVALID_VALUE,
            payloadJson(mapOf("account.balance.amount" to quoted("1"), "account.balance.currency" to quoted("brl"))),
            "account.balance.amount",
        )
    }

    @Test
    fun `a missing field beats an invalid one`() {
        assertRejected(
            RejectionReason.MISSING_FIELD,
            payloadJson(mapOf("transaction.id" to quoted("1-1-1-1-1"), "account.balance.currency" to ABSENT_FIELD)),
            "account.balance.currency",
        )
    }

    @Test
    fun `neither the message nor the detail nor the cause carry values of the payload`() {
        val secret = "SEGREDO-123"
        val payloads =
            listOf(
                payloadJson(mapOf("transaction.currency" to quoted(secret))),
                payloadJson(mapOf("transaction.id" to quoted(secret))),
                payloadJson(mapOf("transaction.type" to quoted(secret))),
                payloadJson(mapOf("transaction.amount" to quoted(secret))),
                payloadJson(mapOf("transaction.timestamp" to quoted(secret))),
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
    fun `a secret in an ignored field is parsed like the payload without it`() {
        assertEquals(parse(payloadJson()), parse(payloadJson(extras = ",\"note\":\"SEGREDO-123\"")))
    }

    private companion object {
        val INVALID_IDENTIFIERS = listOf(quoted("1-1-1-1-1"), "123", quoted(""), quoted("not-a-uuid"), "true", "{}", "[]", quoted(" 8e8ae808-b154-48b5-9f3e-553935cc4543"))

        val INVALID_AMOUNTS = listOf(quoted("10.00"), "true", "{}", "[]", "123456789012345678901234567890123456789", "0.000000000000000000000000000000000000001", "1E999999999", "1E39")

        val NEGATIVE_AMOUNTS = listOf("-0.01", "-1")

        fun documentNestedTo(totalDepth: Int): String = "{\"x\":" + "[".repeat(totalDepth - 1) + "]".repeat(totalDepth - 1) + "}"

        @JvmStatic
        fun brokenShapes(): Stream<Arguments> {
            val transactionObject = Regex("\"transaction\":\\{[^}]*}")
            val balanceObject = Regex("\"balance\":\\{[^}]*}")
            return Stream.of(
                Arguments.of("not json", "{not json"),
                Arguments.of("duplicate key", "{\"transaction\":{\"id\":\"a\"},\"transaction\":{\"id\":\"b\"},\"account\":{}}"),
                Arguments.of("duplicate key deep", payloadJson().replace("\"currency\":\"BRL\"", "\"currency\":\"BRL\",\"currency\":\"USD\"")),
                Arguments.of("tokens after the document", payloadJson() + "{}"),
                Arguments.of("trailing garbage", payloadJson() + "x"),
                Arguments.of("NaN", payloadJson(mapOf("transaction.amount" to "NaN"))),
                Arguments.of("depth 501", documentNestedTo(totalDepth = 501)),
                Arguments.of("number with 1001 chars", payloadJson(mapOf("transaction.amount" to "1".repeat(1001)))),
                Arguments.of("root array", "[]"),
                Arguments.of("root string", "\"x\""),
                Arguments.of("root null", "null"),
                Arguments.of("root number", "42"),
                Arguments.of("transaction array", payloadJson().replace(transactionObject, "\"transaction\":[]")),
                Arguments.of("transaction string", payloadJson().replace(transactionObject, "\"transaction\":\"x\"")),
                Arguments.of("transaction number", payloadJson().replace(transactionObject, "\"transaction\":1")),
                Arguments.of("account string", "{\"transaction\":{},\"account\":\"x\"}"),
                Arguments.of("balance string", payloadJson().replace(balanceObject, "\"balance\":\"x\"")),
                Arguments.of("balance array", payloadJson().replace(balanceObject, "\"balance\":[1]")),
            )
        }
    }
}
