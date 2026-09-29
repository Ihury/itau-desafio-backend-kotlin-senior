package br.com.itau.challenge.balance.adapter.input.web

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.slf4j.MDC
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter
import java.util.UUID

/**
 * Correlacao da requisicao: aceita `X-Correlation-Id` valido (`[A-Za-z0-9._-]{1,64}`) ou gera um UUID, devolve o valor
 * no MESMO cabecalho (inclusive nas respostas de erro, pois e escrito antes da cadeia) e o poe no MDC `correlationId`.
 * O MDC (`correlationId` e `accountId`, este definido pelo controller) e sempre limpo em `finally`.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
class CorrelationIdFilter : OncePerRequestFilter() {
    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain,
    ) {
        val supplied = request.getHeader(HEADER)
        val correlationId = if (supplied != null && VALID.matches(supplied)) supplied else UUID.randomUUID().toString()
        response.setHeader(HEADER, correlationId)
        MDC.put(MDC_CORRELATION_ID, correlationId)
        try {
            filterChain.doFilter(request, response)
        } finally {
            MDC.remove(MDC_CORRELATION_ID)
            MDC.remove(MDC_ACCOUNT_ID)
        }
    }

    companion object {
        const val HEADER = "X-Correlation-Id"
        const val PATTERN = "^[A-Za-z0-9._-]{1,64}$"
        const val MDC_CORRELATION_ID = "correlationId"
        const val MDC_ACCOUNT_ID = "accountId"

        private val VALID = Regex(PATTERN)
    }
}
