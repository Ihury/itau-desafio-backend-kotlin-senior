package br.com.itau.challenge.balance.adapter.input.web

import br.com.itau.challenge.balance.adapter.input.logStoreUnavailable
import br.com.itau.challenge.balance.domain.exception.AccountDisabledException
import br.com.itau.challenge.balance.domain.exception.AccountNotFoundException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreCircuitOpenException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
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

@RestControllerAdvice
class ProblemDetailsAdvice(
    @param:Value($$"${balance.circuit-breaker.open-wait}") private val retryAfter: Duration,
) : ResponseEntityExceptionHandler() {
    @ExceptionHandler(InvalidAccountIdException::class)
    fun invalidAccountId(request: HttpServletRequest): ResponseEntity<ProblemDetail> = problem(request, ProblemType.INVALID_REQUEST)

    @ExceptionHandler(AccountNotFoundException::class)
    fun accountNotFound(request: HttpServletRequest): ResponseEntity<ProblemDetail> = problem(request, ProblemType.ACCOUNT_NOT_FOUND)

    @ExceptionHandler(AccountDisabledException::class)
    fun accountDisabled(request: HttpServletRequest): ResponseEntity<ProblemDetail> = problem(request, ProblemType.ACCOUNT_DISABLED)

    @ExceptionHandler(BalanceStoreUnavailableException::class)
    fun storeUnavailable(
        exception: BalanceStoreUnavailableException,
        request: HttpServletRequest,
    ): ResponseEntity<ProblemDetail> {
        logStoreFailure(exception)
        return problem(request, ProblemType.SERVICE_UNAVAILABLE, retryAfterSeconds())
    }

    @ExceptionHandler(Exception::class)
    fun unexpectedError(
        exception: Exception,
        request: HttpServletRequest,
    ): ResponseEntity<ProblemDetail> {
        log.error("unexpected error while handling request method={}", request.method, exception)
        return problem(request, ProblemType.INTERNAL_ERROR)
    }

    private fun logStoreFailure(exception: BalanceStoreUnavailableException) {
        if (exception is BalanceStoreCircuitOpenException) {
            log.debug("balance read rejected: circuit breaker open")
        } else {
            log.logStoreUnavailable(exception, "balance store unavailable {}", exception.logDescription())
        }
    }

    private fun retryAfterSeconds(): Long = maxOf(1L, ceil(retryAfter.toMillis() / MILLIS_PER_SECOND).toLong())

    private fun problem(
        request: HttpServletRequest,
        type: ProblemType,
        retryAfterSeconds: Long? = null,
    ): ResponseEntity<ProblemDetail> {
        val body = ProblemDetail.forStatusAndDetail(type.status, type.detail)
        body.type = URI.create("$TYPE_PREFIX${type.slug}")
        body.title = type.title
        runCatching { URI(request.requestURI) }.getOrNull()?.let { body.instance = it }
        val headers = HttpHeaders()
        retryAfterSeconds?.let { headers.set(HttpHeaders.RETRY_AFTER, it.toString()) }
        return ResponseEntity.status(type.status).headers(headers).body(body)
    }

    private enum class ProblemType(
        val status: HttpStatus,
        val slug: String,
        val title: String,
        val detail: String,
    ) {
        INVALID_REQUEST(HttpStatus.BAD_REQUEST, "requisicao-invalida", "Requisição inválida", "O identificador da conta deve ser um UUID válido."),
        ACCOUNT_NOT_FOUND(HttpStatus.NOT_FOUND, "conta-nao-encontrada", "Conta não encontrada", "Não há saldo registrado para a conta informada."),
        ACCOUNT_DISABLED(HttpStatus.CONFLICT, "conta-desabilitada", "Conta desabilitada", "A conta está desabilitada e seu saldo não pode ser consultado."),
        SERVICE_UNAVAILABLE(
            HttpStatus.SERVICE_UNAVAILABLE,
            "servico-indisponivel",
            "Serviço indisponível",
            "Não foi possível consultar o saldo agora. Tente novamente em instantes.",
        ),
        INTERNAL_ERROR(
            HttpStatus.INTERNAL_SERVER_ERROR,
            "erro-interno",
            "Erro interno",
            "Ocorreu um erro inesperado. Informe o identificador de correlação ao suporte.",
        ),
    }

    private companion object {
        private val log = LoggerFactory.getLogger(ProblemDetailsAdvice::class.java)
        private const val TYPE_PREFIX = "urn:problem-type:consulta-saldo:"
        private const val MILLIS_PER_SECOND = 1000.0
    }
}
