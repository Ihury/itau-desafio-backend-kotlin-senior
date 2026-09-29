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
 * Parser estrito do evento de transacao (contracts/kafka-events.md secoes 3 e 4). Sobre a arvore JSON, sem DTO tipado nem
 * coercao silenciosa (Constitution IV). Passos, nesta ordem, e a PRIMEIRA falha determina o motivo:
 *
 * 1. payload: nulo/vazio, > 64 KiB, UTF-8 invalido, JSON malformado, chave duplicada, tokens apos o documento, profundidade
 *    > 500, numero com > 1000 caracteres, raiz que nao e objeto, ou `transaction`/`account`/`account.balance` presentes
 *    com tipo diferente de objeto -> `malformed_payload` (sem `detail`);
 * 2. presenca dos 12 campos obrigatorios (ausente ou `null`) -> `missing_field` com o caminho do campo;
 * 3. valores, na ordem fixa `transaction.id`, `type`, `amount`, `currency`, `status`, `timestamp`, `account.id`, `owner`,
 *    `created_at`, `status`, `balance.amount`, `balance.currency`, com o caminho do campo no `detail`.
 *
 * A tolerancia de timestamp futuro nao e daqui: precisa de relogio e e da camada `application`. Campos desconhecidos sao
 * ignorados. O `JsonMapper` e PRIVADO (nao e bean): um bean customizado desativaria o `JsonMapper` do Spring MVC.
 *
 * Privacidade: `detail` e a mensagem nunca contem valores do payload, e a excecao de dominio nunca encadeia a causa do
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

    /** Converte os bytes verbatim do registro no evento de dominio ou lanca [InvalidEventException]. */
    fun parse(bytes: ByteArray?): TransactionEvent {
        val root = readTree(bytes)
        val transaction = objectAt(root, "transaction")
        val account = objectAt(root, "account")
        val balance = account?.let { objectAt(it, "balance") }
        return build(requireFields(transaction, account, balance))
    }

    // ---- passo 1: payload e estrutura

    private fun readTree(bytes: ByteArray?): JsonNode {
        if (bytes == null || bytes.isEmpty() || bytes.size > MAX_PAYLOAD_BYTES) throw malformed()
        val text =
            try {
                STRICT_UTF8.get().decode(ByteBuffer.wrap(bytes)).toString()
            } catch (_: CharacterCodingException) {
                throw malformed()
            }
        val root =
            try {
                mapper.readTree(text)
            } catch (_: JacksonException) {
                throw malformed()
            }
        if (root == null || !root.isObject) throw malformed()
        return root
    }

    /** Objeto filho: ausente/`null` -> `null` (tratado como campo ausente no passo 2); presente e nao objeto -> malformado. */
    private fun objectAt(
        parent: JsonNode,
        name: String,
    ): JsonNode? {
        val child = parent.get(name)
        if (child == null || child.isNull) return null
        if (!child.isObject) throw malformed()
        return child
    }

    // ---- passo 2: presenca

    private fun requireFields(
        transaction: JsonNode?,
        account: JsonNode?,
        balance: JsonNode?,
    ): Fields {
        val tx = transaction ?: throw missing("transaction")
        val txId = present(tx, "transaction", "id")
        val txType = present(tx, "transaction", "type")
        val txAmount = present(tx, "transaction", "amount")
        val txCurrency = present(tx, "transaction", "currency")
        val txStatus = present(tx, "transaction", "status")
        val txTimestamp = present(tx, "transaction", "timestamp")
        val acc = account ?: throw missing("account")
        val accId = present(acc, "account", "id")
        val accOwner = present(acc, "account", "owner")
        val accCreatedAt = present(acc, "account", "created_at")
        val accStatus = present(acc, "account", "status")
        val bal = balance ?: throw missing("account.balance")
        val balAmount = present(bal, "account.balance", "amount")
        val balCurrency = present(bal, "account.balance", "currency")
        return Fields(txId, txType, txAmount, txCurrency, txStatus, txTimestamp, accId, accOwner, accCreatedAt, accStatus, balAmount, balCurrency)
    }

    private fun present(
        parent: JsonNode,
        parentPath: String,
        name: String,
    ): JsonNode {
        val node = parent.get(name)
        if (node == null || node.isNull) throw missing("$parentPath.$name")
        return node
    }

    // ---- passo 3: valores, na ordem fixa

    private fun build(f: Fields): TransactionEvent {
        val transactionId = field("transaction.id") { TransactionId.parse(text(f.txId, RejectionReason.INVALID_IDENTIFIER)) }
        val type = field("transaction.type") { TransactionType.parse(text(f.txType, RejectionReason.UNKNOWN_DOMAIN_VALUE)) }
        val amount = field("transaction.amount") { Transaction.validTransactionAmount(decimal(f.txAmount)) }
        val currency = field("transaction.currency") { CurrencyCode.parse(text(f.txCurrency, RejectionReason.INVALID_CURRENCY)) }
        val status = field("transaction.status") { TransactionStatus.parse(text(f.txStatus, RejectionReason.UNKNOWN_DOMAIN_VALUE)) }
        val timestamp = field("transaction.timestamp") { EventInstant.transactionTimestamp(micros(f.txTimestamp), minEventTimestamp) }
        val accountId = field("account.id") { AccountId.parse(text(f.accId, RejectionReason.INVALID_IDENTIFIER)) }
        val owner = field("account.owner") { OwnerId.parse(text(f.accOwner, RejectionReason.INVALID_IDENTIFIER)) }
        val createdAt = field("account.created_at") { EventInstant.accountCreatedAt(micros(f.accCreatedAt), minAccountCreatedAt) }
        val accountStatus = field("account.status") { AccountStatus.parse(text(f.accStatus, RejectionReason.UNKNOWN_DOMAIN_VALUE)) }
        val balanceAmount = field("account.balance.amount") { validatedAmount(decimal(f.balAmount)) }
        val balanceCurrency = field("account.balance.currency") { CurrencyCode.parse(text(f.balCurrency, RejectionReason.INVALID_CURRENCY)) }
        return TransactionEvent(
            transaction = Transaction(transactionId, type, amount, currency, status, timestamp),
            account = AccountState(accountId, owner, createdAt, accountStatus, Money.of(balanceAmount, balanceCurrency)),
        )
    }

    /** Executa a conversao de um campo e anexa o caminho a qualquer rejeicao do dominio. */
    private inline fun <T> field(
        path: String,
        block: () -> T,
    ): T =
        try {
            block()
        } catch (failure: InvalidEventException) {
            throw failure.withDetail(path)
        }

    /** Texto do no; qualquer outro tipo JSON e rejeitado com o motivo do campo (sem coercao). */
    private fun text(
        node: JsonNode,
        onWrongType: RejectionReason,
    ): String = if (node.isString) node.stringValue() else throw InvalidEventException(onWrongType)

    /** Numero JSON (nunca string/booleano/objeto); decimais chegam como `BigDecimal`, jamais por `double`. */
    private fun decimal(node: JsonNode): BigDecimal =
        if (node.isNumber) node.decimalValue() else throw InvalidEventException(RejectionReason.INVALID_VALUE)

    /** Inteiro JSON que cabe em `long`: `1751641364589998.0` e `1.75E15` (ponto flutuante) e strings sao rejeitados. */
    private fun micros(node: JsonNode): Long =
        if (node.isIntegralNumber && node.canConvertToLong()) node.longValue() else throw InvalidEventException(RejectionReason.INVALID_TIMESTAMP)

    private fun malformed() = InvalidEventException(RejectionReason.MALFORMED_PAYLOAD)

    private fun missing(path: String) = InvalidEventException(RejectionReason.MISSING_FIELD, path)

    private class Fields(
        val txId: JsonNode,
        val txType: JsonNode,
        val txAmount: JsonNode,
        val txCurrency: JsonNode,
        val txStatus: JsonNode,
        val txTimestamp: JsonNode,
        val accId: JsonNode,
        val accOwner: JsonNode,
        val accCreatedAt: JsonNode,
        val accStatus: JsonNode,
        val balAmount: JsonNode,
        val balCurrency: JsonNode,
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
