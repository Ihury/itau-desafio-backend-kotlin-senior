package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals

class IdentifiersTest {
    private val canonical = "5b19c8b6-0cc4-4c72-a989-0c2ee15fa975"

    private val parsers: Map<String, (String) -> Any> =
        mapOf(
            "AccountId" to { raw -> AccountId.parse(raw) },
            "TransactionId" to { raw -> TransactionId.parse(raw) },
            "OwnerId" to { raw -> OwnerId.parse(raw) },
        )

    private val invalidInputs =
        listOf(
            "1-1-1-1-1",
            "",
            "5b19c8b60cc44c72a9890c2ee15fa975",
            "5b19c8b6-0cc4-4c72-a989-0c2ee15fa97g",
            "{5b19c8b6-0cc4-4c72-a989-0c2ee15fa975}",
            "urn:uuid:5b19c8b6-0cc4-4c72-a989-0c2ee15fa975",
            " 5b19c8b6-0cc4-4c72-a989-0c2ee15fa975",
            "5b19c8b6-0cc4-4c72-a989-0c2ee15fa975 ",
            "5b19c8b6-0cc4-4c72-a989-0c2ee15fa9755",
            "5b19c8b6-0cc4-4c72-a989-0c2ee15fa97",
        )

    @Test
    fun `the JDK parser is lenient, which is why the strict regex exists`() {
        assertEquals(UUID.fromString("1-1-1-1-1"), UUID.fromString("00000001-0001-0001-0001-000000000001"))
    }

    @Test
    fun `accepts canonical lowercase and uppercase identifiers and normalizes to lowercase`() {
        assertEquals(canonical, AccountId.parse(canonical).value)
        assertEquals(canonical, AccountId.parse(canonical.uppercase()).value)
        assertEquals(canonical, TransactionId.parse(canonical.uppercase()).value)
        assertEquals(canonical, OwnerId.parse("5B19C8B6-0cc4-4C72-a989-0C2EE15FA975").value)
    }

    @Test
    fun `rejects malformed identifiers with invalid_identifier for every identifier type`() {
        parsers.forEach { (name, parse) ->
            invalidInputs.forEach { raw ->
                val error = assertFailsWith<InvalidEventException>("$name aceitou '$raw'") { parse(raw) }
                assertEquals(RejectionReason.INVALID_IDENTIFIER, error.reason, "$name com '$raw'")
            }
        }
    }

    @Test
    fun `equality is by canonical value regardless of input case`() {
        assertEquals(AccountId.parse(canonical), AccountId.parse(canonical.uppercase()))
        assertEquals(AccountId.parse(canonical).hashCode(), AccountId.parse(canonical.uppercase()).hashCode())
        assertNotEquals(
            AccountId.parse(canonical),
            AccountId.parse("8e8ae808-b154-48b5-9f3e-553935cc4543"),
        )
    }

    @Test
    fun `toString exposes only the canonical value`() {
        assertEquals(canonical, AccountId.parse(canonical.uppercase()).toString())
        assertEquals(canonical, TransactionId.parse(canonical).toString())
        assertEquals(canonical, OwnerId.parse(canonical).toString())
    }

    @Test
    fun `boxed identifiers in collections keep canonical value and deduplicate`() {
        val upper = canonical.uppercase()
        val accounts = listOf(AccountId.parse(canonical), AccountId.parse(upper))
        val transactions = listOf(TransactionId.parse(canonical), TransactionId.parse(upper))
        val owners = listOf(OwnerId.parse(canonical), OwnerId.parse(upper))

        assertEquals(1, accounts.toSet().size)
        assertEquals(listOf(canonical, canonical), accounts.map { it.value })
        assertEquals(listOf(canonical, canonical), accounts.asAnyList().map { it.toString() })
        assertEquals(listOf(canonical, canonical), transactions.map { it.value })
        assertEquals(listOf(canonical, canonical), transactions.asAnyList().map { it.toString() })
        assertEquals(listOf(canonical, canonical), owners.map { it.value })
        assertEquals(listOf(canonical, canonical), owners.asAnyList().map { it.toString() })
    }

    private fun List<Any>.asAnyList(): List<Any> = this
}
