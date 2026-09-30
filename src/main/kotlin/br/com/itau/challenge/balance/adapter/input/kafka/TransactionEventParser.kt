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

/**
 * Parser estrito do evento de transacao, sobre a arvore JSON, sem DTO tipado nem coercao silenciosa. Passos, nesta ordem; a
 * PRIMEIRA falha determina o motivo:
 *
 * 1. payload: nulo/vazio, > 64 KiB, UTF-8 invalido, JSON malformado, chave duplicada, tokens apos o documento, profundidade
 *    > 500, numero com > 1000 caracteres, raiz que nao e objeto, ou `transaction`/`account`/`account.balance` presentes
 *    com tipo diferente de objeto -> `malformed_payload` (sem `fieldPath`);
 * 2. presenca dos 12 campos obrigatorios (ausente ou `null`) -> `missing_field` com o caminho do campo;
 * 3. valores, na ordem de [toEvent], com o caminho do campo em `fieldPath`.
 *
 * A tolerancia de timestamp futuro nao e daqui: precisa de relogio e e da camada `application`. Campos desconhecidos sao
 * ignorados. O `JsonMapper` e privado (nao e bean): um bean customizado desativaria o `JsonMapper` do Spring MVC.
 *
 * Privacidade: `fieldPath` e a mensagem nunca contem valores do payload, e a excecao de dominio nunca encadeia a causa do
 * parser (mensagens de parsers podem citar trechos do payload).
 */
class TransactionEventParser(
    private val minEventTimestamp: Instant,
    private val minAccountCreatedAt: Instant,
) {
    private val mapper: JsonMapper =
        JsonMapper
            .builder(
                JsonFactory
                    .builder()
                    .streamReadConstraints(
                        StreamReadConstraints
                            .builder()
                            .maxNestingDepth(MAX_NESTING_DEPTH)
                            .maxNumberLength(MAX_NUMBER_LENGTH)
                            .build(),
                    ).enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                    .build(),
            ).enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .build()

    /** @throws InvalidEventException payload invalido */
    fun parse(bytes: ByteArray?): TransactionEvent {
        val root = readTree(bytes)
        val transaction = optionalObjectAt(root, "transaction")
        val account = optionalObjectAt(root, "account")
        val balance = account?.let { optionalObjectAt(it, "balance") }
        return toEvent(requirePresence(transaction, account, balance))
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

    /** Objeto filho: ausente/`null` -> `null` (tratado como campo ausente no passo 2); presente e nao objeto -> malformado. */
    private fun optionalObjectAt(
        parent: JsonNode,
        name: String,
    ): JsonNode? {
        val child = parent.get(name)
        if (child == null || child.isNull) return null
        if (!child.isObject) throw malformedPayload()
        return child
    }

    private fun requirePresence(
        transaction: JsonNode?,
        account: JsonNode?,
        balance: JsonNode?,
    ): RequiredNodes {
        val transactionNode = transaction ?: throw missingField("transaction")
        val transactionId = requiredChild(transactionNode, "transaction", "id")
        val transactionType = requiredChild(transactionNode, "transaction", "type")
        val transactionAmount = requiredChild(transactionNode, "transaction", "amount")
        val transactionCurrency = requiredChild(transactionNode, "transaction", "currency")
        val transactionStatus = requiredChild(transactionNode, "transaction", "status")
        val transactionTimestamp = requiredChild(transactionNode, "transaction", "timestamp")
        val accountNode = account ?: throw missingField("account")
        val accountId = requiredChild(accountNode, "account", "id")
        val accountOwner = requiredChild(accountNode, "account", "owner")
        val accountCreatedAt = requiredChild(accountNode, "account", "created_at")
        val accountStatus = requiredChild(accountNode, "account", "status")
        val balanceNode = balance ?: throw missingField("account.balance")
        val balanceAmount = requiredChild(balanceNode, "account.balance", "amount")
        val balanceCurrency = requiredChild(balanceNode, "account.balance", "currency")
        return RequiredNodes(
            transactionId,
            transactionType,
            transactionAmount,
            transactionCurrency,
            transactionStatus,
            transactionTimestamp,
            accountId,
            accountOwner,
            accountCreatedAt,
            accountStatus,
            balanceAmount,
            balanceCurrency,
        )
    }

    private fun requiredChild(
        parent: JsonNode,
        parentPath: String,
        name: String,
    ): JsonNode {
        val node = parent.get(name)
        if (node == null || node.isNull) throw missingField("$parentPath.$name")
        return node
    }

    private fun toEvent(nodes: RequiredNodes): TransactionEvent {
        val transactionId = atFieldPath("transaction.id") { TransactionId.parse(text(nodes.transactionId, RejectionReason.INVALID_IDENTIFIER)) }
        val type = atFieldPath("transaction.type") { TransactionType.parse(text(nodes.transactionType, RejectionReason.UNKNOWN_DOMAIN_VALUE)) }
        val amount = atFieldPath("transaction.amount") { Transaction.validatedTransactionAmount(decimal(nodes.transactionAmount)) }
        val currency = atFieldPath("transaction.currency") { CurrencyCode.parse(text(nodes.transactionCurrency, RejectionReason.INVALID_CURRENCY)) }
        val status = atFieldPath("transaction.status") { TransactionStatus.parse(text(nodes.transactionStatus, RejectionReason.UNKNOWN_DOMAIN_VALUE)) }
        val timestamp = atFieldPath("transaction.timestamp") { EventInstant.transactionTimestamp(epochMicros(nodes.transactionTimestamp), minEventTimestamp) }
        val accountId = atFieldPath("account.id") { AccountId.parse(text(nodes.accountId, RejectionReason.INVALID_IDENTIFIER)) }
        val owner = atFieldPath("account.owner") { OwnerId.parse(text(nodes.accountOwner, RejectionReason.INVALID_IDENTIFIER)) }
        val createdAt = atFieldPath("account.created_at") { EventInstant.accountCreatedAt(epochMicros(nodes.accountCreatedAt), minAccountCreatedAt) }
        val accountStatus = atFieldPath("account.status") { AccountStatus.parse(text(nodes.accountStatus, RejectionReason.UNKNOWN_DOMAIN_VALUE)) }
        val balanceAmount = atFieldPath("account.balance.amount") { validatedAmount(decimal(nodes.balanceAmount)) }
        val balanceCurrency = atFieldPath("account.balance.currency") { CurrencyCode.parse(text(nodes.balanceCurrency, RejectionReason.INVALID_CURRENCY)) }
        return TransactionEvent(
            transaction = Transaction(transactionId, type, amount, currency, status, timestamp),
            account = AccountState(accountId, owner, createdAt, accountStatus, Money.of(balanceAmount, balanceCurrency)),
        )
    }

    private inline fun <T> atFieldPath(
        path: String,
        block: () -> T,
    ): T =
        try {
            block()
        } catch (failure: InvalidEventException) {
            throw failure.withFieldPath(path)
        }

    /** Sem coercao: outro tipo JSON e rejeitado com [onWrongType]. */
    private fun text(
        node: JsonNode,
        onWrongType: RejectionReason,
    ): String = if (node.isString) node.stringValue() else throw InvalidEventException(onWrongType)

    /** Numero JSON (nunca string/booleano/objeto); decimais chegam como `BigDecimal`, jamais por `double`. */
    private fun decimal(node: JsonNode): BigDecimal =
        if (node.isNumber) node.decimalValue() else throw InvalidEventException(RejectionReason.INVALID_VALUE)

    /** Inteiro JSON que cabe em `long`: `1751641364589998.0` e `1.75E15` (ponto flutuante) e strings sao rejeitados. */
    private fun epochMicros(node: JsonNode): Long =
        if (node.isIntegralNumber && node.canConvertToLong()) node.longValue() else throw InvalidEventException(RejectionReason.INVALID_TIMESTAMP)

    private fun malformedPayload() = InvalidEventException(RejectionReason.MALFORMED_PAYLOAD)

    private fun missingField(path: String) = InvalidEventException(RejectionReason.MISSING_FIELD, path)

    private class RequiredNodes(
        val transactionId: JsonNode,
        val transactionType: JsonNode,
        val transactionAmount: JsonNode,
        val transactionCurrency: JsonNode,
        val transactionStatus: JsonNode,
        val transactionTimestamp: JsonNode,
        val accountId: JsonNode,
        val accountOwner: JsonNode,
        val accountCreatedAt: JsonNode,
        val accountStatus: JsonNode,
        val balanceAmount: JsonNode,
        val balanceCurrency: JsonNode,
    )

    private companion object {
        const val MAX_PAYLOAD_BYTES = 64 * 1024
        const val MAX_NESTING_DEPTH = 500
        const val MAX_NUMBER_LENGTH = 1000

        /** `CharsetDecoder` nao e thread-safe: um por thread do listener. */
        val STRICT_UTF8: ThreadLocal<CharsetDecoder> =
            ThreadLocal.withInitial {
                Charsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
            }
    }
}
