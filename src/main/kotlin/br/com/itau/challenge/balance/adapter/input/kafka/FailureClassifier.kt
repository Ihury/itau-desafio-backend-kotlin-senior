package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.RejectionReason
import org.springframework.kafka.listener.ListenerExecutionFailedException

enum class FailureClass {
    /** Evento invalido: nunca vai funcionar, e isolado no DLT na primeira falha. */
    PERMANENT,

    /** Armazenamento indisponivel: retentada sem fim com backoff, NUNCA vai ao DLT (a mensagem valida fica no broker). */
    TRANSIENT,

    /** Qualquer outra excecao (defeito nosso, rejeicao de validacao do armazenamento): 3 entregas e DLT `unprocessable_event`. */
    UNCLASSIFIED,
}

/** Motivo e caminho do campo com que uma mensagem e isolada no DLT; nunca carrega valores do payload. */
data class Rejection(
    val reason: RejectionReason,
    val fieldPath: String?,
)

/**
 * O container embrulha a excecao do listener em [ListenerExecutionFailedException]; ela e desembrulhada antes de
 * classificar. A classificacao e por classe, nunca por mensagem.
 */
object FailureClassifier {
    fun classify(failure: Throwable): FailureClass =
        when (unwrap(failure)) {
            is InvalidEventException -> FailureClass.PERMANENT
            is BalanceStoreUnavailableException -> FailureClass.TRANSIENT
            else -> FailureClass.UNCLASSIFIED
        }

    /** Motivo do isolamento: o do proprio evento invalido ou, para qualquer outra falha, `unprocessable_event` sem detalhe. */
    fun rejectionOf(failure: Throwable): Rejection =
        when (val root = unwrap(failure)) {
            is InvalidEventException -> Rejection(root.reason, root.fieldPath)
            else -> Rejection(RejectionReason.UNPROCESSABLE_EVENT, null)
        }

    /** Remove os embrulhos do listener (pode haver mais de um nivel); o resto da cadeia de causas e preservado. */
    fun unwrap(failure: Throwable): Throwable {
        var current = failure
        while (current is ListenerExecutionFailedException) {
            current = current.cause ?: return current
        }
        return current
    }
}
