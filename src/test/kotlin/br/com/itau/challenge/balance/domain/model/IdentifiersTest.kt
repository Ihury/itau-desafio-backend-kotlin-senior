package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_ACCOUNT_ID
import br.com.itau.challenge.balance.testing.boxedAsAny
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals

class IdentifiersTest {
    private val canonicalId = DEFAULT_ACCOUNT_ID

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
    fun `accepts canonicalId lowercase and uppercase identifiers and normalizes to lowercase`() {
        assertEquals(canonicalId, AccountId.parse(canonicalId).value)
        assertEquals(canonicalId, AccountId.parse(canonicalId.uppercase()).value)
        assertEquals(canonicalId, TransactionId.parse(canonicalId.uppercase()).value)
        assertEquals(canonicalId, OwnerId.parse("5B19C8B6-0cc4-4C72-a989-0C2EE15FA975").value)
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
    fun `equality is by canonicalId value regardless of input case`() {
        assertEquals(AccountId.parse(canonicalId), AccountId.parse(canonicalId.uppercase()))
        assertEquals(AccountId.parse(canonicalId).hashCode(), AccountId.parse(canonicalId.uppercase()).hashCode())
        assertNotEquals(
            AccountId.parse(canonicalId),
            AccountId.parse("8e8ae808-b154-48b5-9f3e-553935cc4543"),
        )
    }

    @Test
    fun `toString exposes only the canonicalId value`() {
        assertEquals(canonicalId, AccountId.parse(canonicalId.uppercase()).toString())
        assertEquals(canonicalId, TransactionId.parse(canonicalId).toString())
        assertEquals(canonicalId, OwnerId.parse(canonicalId).toString())
    }

    @Test
    fun `boxed identifiers in collections keep canonicalId value and deduplicate`() {
        val upper = canonicalId.uppercase()
        val accounts = listOf(AccountId.parse(canonicalId), AccountId.parse(upper))
        val transactions = listOf(TransactionId.parse(canonicalId), TransactionId.parse(upper))
        val owners = listOf(OwnerId.parse(canonicalId), OwnerId.parse(upper))

        assertEquals(1, accounts.toSet().size)
        assertEquals(listOf(canonicalId, canonicalId), accounts.map { it.value })
        assertEquals(listOf(canonicalId, canonicalId), accounts.boxedAsAny().map { it.toString() })
        assertEquals(listOf(canonicalId, canonicalId), transactions.map { it.value })
        assertEquals(listOf(canonicalId, canonicalId), transactions.boxedAsAny().map { it.toString() })
        assertEquals(listOf(canonicalId, canonicalId), owners.map { it.value })
        assertEquals(listOf(canonicalId, canonicalId), owners.boxedAsAny().map { it.toString() })
    }
}
