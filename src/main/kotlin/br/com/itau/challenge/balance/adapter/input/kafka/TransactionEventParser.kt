package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.AccountState
import br.com.itau.challenge.balance.domain.model.AccountStatus
import br.com.itau.challenge.balance.domain.model.CurrencyCode
import br.com.itau.challenge.balance.domain.model.EventInstant
import br.com.itau.challenge.balance.domain.model.Money
import br.com.itau.challenge.balance.domain.model.OwnerId
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.Transaction
import br.com.itau.challenge.balance.domain.model.TransactionEvent
import br.com.itau.challenge.balance.domain.model.TransactionId
import br.com.itau.challenge.balance.domain.model.TransactionStatus
import br.com.itau.challenge.balance.domain.model.TransactionType
import br.com.itau.challenge.balance.domain.model.validatedAmount
import tools.jackson.core.JacksonException
import tools.jackson.core.StreamReadConstraints
import tools.jackson.core.StreamReadFeature
import tools.jackson.core.json.JsonFactory
import tools.jackson.databind.DeserializationFeature
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CharsetDecoder
import java.nio.charset.CodingErrorAction
import java.time.Instant

class TransactionEventParser(
    private val minEventTimestamp: Instant,
    private val minAccountCreatedAt: Instant,
) {
    private val mapper: JsonMapper = strictJsonMapper()

    fun parse(bytes: ByteArray?): TransactionEvent {
        val root = readTree(bytes)
        val transaction = optionalObjectAt(root, TRANSACTION)
        val account = optionalObjectAt(root, ACCOUNT)
        val balance = account?.let { optionalObjectAt(it, BALANCE_FIELD) }
        return toEvent(requirePresence(mapOf(TRANSACTION to transaction, ACCOUNT to account, BALANCE_PATH to balance)))
    }

    private fun readTree(bytes: ByteArray?): JsonNode {
        if (bytes == null || bytes.isEmpty() || bytes.size > MAX_PAYLOAD_BYTES) throw malformedPayload()
        val text =
            try {
                STRICT_UTF8.get().decode(ByteBuffer.wrap(bytes)).toString()
            } catch (_: CharacterCodingException) {
                throw malformedPayload()
            }
        val root =
            try {
                mapper.readTree(text)
            } catch (_: JacksonException) {
                throw malformedPayload()
            }
        if (root == null || !root.isObject) throw malformedPayload()
        return root
    }

    private fun optionalObjectAt(
        parent: JsonNode,
        name: String,
    ): JsonNode? {
        val child = parent.get(name)
        if (child == null || child.isNull) return null
        if (!child.isObject) throw malformedPayload()
        return child
    }

    private fun requirePresence(objects: Map<String, JsonNode?>): RequiredNodes =
        RequiredNodes(
            Field.entries.associateWith { field ->
                val parent = objects[field.parentPath] ?: throw missingField(field.parentPath)
                val node = parent.get(field.childName)
                if (node == null || node.isNull) throw missingField(field.path)
                node
            },
        )

    private fun toEvent(nodes: RequiredNodes): TransactionEvent {
        val transactionId = nodes.convert(Field.TRANSACTION_ID) { TransactionId.parse(text(it, RejectionReason.INVALID_IDENTIFIER)) }
        val type = nodes.convert(Field.TRANSACTION_TYPE) { TransactionType.parse(text(it, RejectionReason.UNKNOWN_DOMAIN_VALUE)) }
        val amount = nodes.convert(Field.TRANSACTION_AMOUNT) { Transaction.validatedTransactionAmount(decimal(it)) }
        val currency = nodes.convert(Field.TRANSACTION_CURRENCY) { CurrencyCode.parse(text(it, RejectionReason.INVALID_CURRENCY)) }
        val status = nodes.convert(Field.TRANSACTION_STATUS) { TransactionStatus.parse(text(it, RejectionReason.UNKNOWN_DOMAIN_VALUE)) }
        val timestamp = nodes.convert(Field.TRANSACTION_TIMESTAMP) { EventInstant.transactionTimestamp(epochMicros(it), minEventTimestamp) }
        val accountId = nodes.convert(Field.ACCOUNT_ID) { AccountId.parse(text(it, RejectionReason.INVALID_IDENTIFIER)) }
        val owner = nodes.convert(Field.ACCOUNT_OWNER) { OwnerId.parse(text(it, RejectionReason.INVALID_IDENTIFIER)) }
        val createdAt = nodes.convert(Field.ACCOUNT_CREATED_AT) { EventInstant.accountCreatedAt(epochMicros(it), minAccountCreatedAt) }
        val accountStatus = nodes.convert(Field.ACCOUNT_STATUS) { AccountStatus.parse(text(it, RejectionReason.UNKNOWN_DOMAIN_VALUE)) }
        val balanceAmount = nodes.convert(Field.BALANCE_AMOUNT) { validatedAmount(decimal(it)) }
        val balanceCurrency = nodes.convert(Field.BALANCE_CURRENCY) { CurrencyCode.parse(text(it, RejectionReason.INVALID_CURRENCY)) }
        return TransactionEvent(
            transaction = Transaction(transactionId, type, amount, currency, status, timestamp),
            account = AccountState(accountId, owner, createdAt, accountStatus, Money.of(balanceAmount, balanceCurrency)),
        )
    }

    private fun text(
        node: JsonNode,
        onWrongType: RejectionReason,
    ): String = if (node.isString) node.stringValue() else throw InvalidEventException(onWrongType)

    private fun decimal(node: JsonNode): BigDecimal =
        if (node.isNumber) node.decimalValue() else throw InvalidEventException(RejectionReason.INVALID_VALUE)

    private fun epochMicros(node: JsonNode): Long =
        if (node.isIntegralNumber && node.canConvertToLong()) node.longValue() else throw InvalidEventException(RejectionReason.INVALID_TIMESTAMP)

    private fun malformedPayload() = InvalidEventException(RejectionReason.MALFORMED_PAYLOAD)

    private fun missingField(path: String) = InvalidEventException(RejectionReason.MISSING_FIELD, path)

    private enum class Field(
        val path: String,
    ) {
        TRANSACTION_ID("transaction.id"),
        TRANSACTION_TYPE("transaction.type"),
        TRANSACTION_AMOUNT("transaction.amount"),
        TRANSACTION_CURRENCY("transaction.currency"),
        TRANSACTION_STATUS("transaction.status"),
        TRANSACTION_TIMESTAMP("transaction.timestamp"),
        ACCOUNT_ID("account.id"),
        ACCOUNT_OWNER("account.owner"),
        ACCOUNT_CREATED_AT("account.created_at"),
        ACCOUNT_STATUS("account.status"),
        BALANCE_AMOUNT("account.balance.amount"),
        BALANCE_CURRENCY("account.balance.currency"),
        ;

        val parentPath: String get() = path.substringBeforeLast('.')
        val childName: String get() = path.substringAfterLast('.')
    }

    private class RequiredNodes(
        private val nodes: Map<Field, JsonNode>,
    ) {
        fun <T> convert(
            field: Field,
            conversion: (JsonNode) -> T,
        ): T =
            try {
                conversion(nodes.getValue(field))
            } catch (failure: InvalidEventException) {
                throw failure.withFieldPath(field.path)
            }
    }

    private companion object {
        const val MAX_PAYLOAD_BYTES = 64 * 1024
        const val MAX_NESTING_DEPTH = 500
        const val MAX_NUMBER_LENGTH = 1000
        const val TRANSACTION = "transaction"
        const val ACCOUNT = "account"
        const val BALANCE_FIELD = "balance"
        const val BALANCE_PATH = "$ACCOUNT.$BALANCE_FIELD"

        val STRICT_UTF8: ThreadLocal<CharsetDecoder> = ThreadLocal.withInitial { strictUtf8Decoder() }

        fun strictUtf8Decoder(): CharsetDecoder =
            Charsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)

        fun strictJsonMapper(): JsonMapper =
            JsonMapper
                .builder(strictJsonFactory())
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .build()

        fun strictJsonFactory(): JsonFactory =
            JsonFactory
                .builder()
                .streamReadConstraints(
                    StreamReadConstraints
                        .builder()
                        .maxNestingDepth(MAX_NESTING_DEPTH)
                        .maxNumberLength(MAX_NUMBER_LENGTH)
                        .build(),
                ).enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .build()
    }
}
