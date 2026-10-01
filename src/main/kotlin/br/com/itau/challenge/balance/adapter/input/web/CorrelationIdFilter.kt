package br.com.itau.challenge.balance.adapter.input.web

import br.com.itau.challenge.balance.adapter.input.MdcKeys
import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.slf4j.MDC
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter
import java.util.UUID

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
class CorrelationIdFilter : OncePerRequestFilter() {
    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain,
    ) {
        val correlationId = resolveCorrelationId(request)
        response.setHeader(HEADER, correlationId)
        MDC.put(MdcKeys.CORRELATION_ID, correlationId)
        try {
            filterChain.doFilter(request, response)
        } finally {
            MDC.remove(MdcKeys.CORRELATION_ID)
            MDC.remove(MdcKeys.ACCOUNT_ID)
        }
    }

    private fun resolveCorrelationId(request: HttpServletRequest): String =
        request.getHeader(HEADER)?.takeIf(VALID_ID::matches) ?: UUID.randomUUID().toString()

    companion object {
        const val HEADER = "X-Correlation-Id"
        const val PATTERN = "^[A-Za-z0-9._-]{1,64}$"

        private val VALID_ID = Regex(PATTERN)
    }
}
