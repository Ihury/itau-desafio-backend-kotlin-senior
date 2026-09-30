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
 * Aceita `X-Correlation-Id` valido ou gera um UUID. O cabecalho de resposta e escrito antes da cadeia, para constar tambem
 * nas respostas de erro. Limpa o MDC (`correlationId` e `accountId`, este definido pelo controller) em `finally`.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
class CorrelationIdFilter : OncePerRequestFilter() {
    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain,
    ) {
        val suppliedId = request.getHeader(HEADER)
        val correlationId = if (suppliedId != null && VALID_ID.matches(suppliedId)) suppliedId else UUID.randomUUID().toString()
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

        private val VALID_ID = Regex(PATTERN)
    }
}
