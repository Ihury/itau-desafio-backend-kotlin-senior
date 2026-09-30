package br.com.itau.challenge.balance.adapter.input.web

import br.com.itau.challenge.balance.domain.exception.AccountDisabledException
import br.com.itau.challenge.balance.domain.exception.AccountNotFoundException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreCircuitOpenException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import jakarta.servlet.http.HttpServletRequest
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ProblemDetail
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler
import java.net.URI
import java.time.Duration
import kotlin.math.ceil

/**
 * Erros no formato Problem Details (RFC 9457, `application/problem+json`). `type` e uma URN estavel
 * (`urn:problem-type:consulta-saldo:<slug>`); o `detail` e uma mensagem fixa: nunca pilha, nome de infraestrutura,
 * saldo, titular nem a mensagem da excecao. Excecoes do framework (404 de rota, 405...) herdam o tratamento de
 * [ResponseEntityExceptionHandler]. Qualquer outra excecao (inclusive `IllegalStateException` de item corrompido e
 * `InvalidEventException` vinda do caso de uso) e erro interno: 500 generico, com o detalhe apenas no log.
 */
@RestControllerAdvice
class ProblemDetailsAdvice(
    @param:Value($$"${balance.circuit-breaker.open-wait}") private val retryAfter: Duration,
) : ResponseEntityExceptionHandler() {
    @ExceptionHandler(InvalidAccountIdException::class)
    fun invalidAccountId(request: HttpServletRequest): ResponseEntity<ProblemDetail> =
        problem(request, HttpStatus.BAD_REQUEST, "requisicao-invalida", "Requisição inválida", "O identificador da conta deve ser um UUID válido.")

    @ExceptionHandler(AccountNotFoundException::class)
    fun accountNotFound(request: HttpServletRequest): ResponseEntity<ProblemDetail> =
        problem(request, HttpStatus.NOT_FOUND, "conta-nao-encontrada", "Conta não encontrada", "Não há saldo registrado para a conta informada.")

    @ExceptionHandler(AccountDisabledException::class)
    fun accountDisabled(request: HttpServletRequest): ResponseEntity<ProblemDetail> =
        problem(
            request,
            HttpStatus.CONFLICT,
            "conta-desabilitada",
            "Conta desabilitada",
            "A conta está desabilitada e seu saldo não pode ser consultado.",
        )

    @ExceptionHandler(BalanceStoreUnavailableException::class)
    fun storeUnavailable(
        exception: BalanceStoreUnavailableException,
        request: HttpServletRequest,
    ): ResponseEntity<ProblemDetail> {
        // A rejeicao com o circuito aberto e esperada e ocorre por requisicao: DEBUG, sem pilha. O WARN fica para as falhas REAIS de
        // leitura e para as transicoes de estado do breaker (`ResilienceConfig`). O diagnostico nunca traz a mensagem livre do SDK;
        // configuracao/credencial (MISCONFIGURED) sobe a ERROR: o 503 e o mesmo, mas exige acao de quem opera.
        when {
            exception is BalanceStoreCircuitOpenException -> log.debug("balance read rejected: circuit breaker open")
            exception.failureCause == StoreFailureCause.MISCONFIGURED -> log.error("balance store misconfigured {}", exception.logDescription())
            else -> log.warn("balance store unavailable {}", exception.logDescription())
        }
        return problem(
            request,
            HttpStatus.SERVICE_UNAVAILABLE,
            "servico-indisponivel",
            "Serviço indisponível",
            "Não foi possível consultar o saldo agora. Tente novamente em instantes.",
            retryAfterSeconds = retryAfterSeconds(),
        )
    }

    @ExceptionHandler(Exception::class)
    fun unexpectedError(
        exception: Exception,
        request: HttpServletRequest,
    ): ResponseEntity<ProblemDetail> {
        log.error("unexpected error while handling request method={}", request.method, exception)
        return problem(
            request,
            HttpStatus.INTERNAL_SERVER_ERROR,
            "erro-interno",
            "Erro interno",
            "Ocorreu um erro inesperado. Informe o identificador de correlação ao suporte.",
        )
    }

    private fun retryAfterSeconds(): Long = maxOf(1L, ceil(retryAfter.toMillis() / MILLIS_PER_SECOND).toLong())

    @Suppress("LongParameterList")
    private fun problem(
        request: HttpServletRequest,
        status: HttpStatus,
        slug: String,
        title: String,
        detail: String,
        retryAfterSeconds: Long? = null,
    ): ResponseEntity<ProblemDetail> {
        val body = ProblemDetail.forStatusAndDetail(status, detail)
        body.type = URI.create("$TYPE_PREFIX$slug")
        body.title = title
        runCatching { URI(request.requestURI) }.getOrNull()?.let { body.instance = it }
        val headers = HttpHeaders()
        retryAfterSeconds?.let { headers.set(HttpHeaders.RETRY_AFTER, it.toString()) }
        return ResponseEntity.status(status).headers(headers).body(body)
    }

    private companion object {
        private val log = LoggerFactory.getLogger(ProblemDetailsAdvice::class.java)
        private const val TYPE_PREFIX = "urn:problem-type:consulta-saldo:"
        private const val MILLIS_PER_SECOND = 1000.0
    }
}
