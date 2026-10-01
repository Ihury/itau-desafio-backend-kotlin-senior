package br.com.itau.challenge.balance.adapter.input.web

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.input.GetBalanceUseCase
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_ACCOUNT_ID
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.transactionEvent
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.context.annotation.Import
import org.springframework.http.HttpStatus
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.MvcResult
import org.springframework.test.web.servlet.ResultActions
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.header
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

@WebMvcTest(BalanceController::class)
@Import(ProblemDetailsAdvice::class, CorrelationIdFilter::class, WebTestBeans::class)
abstract class WebSliceTest {
    @Autowired
    protected lateinit var mockMvc: MockMvc

    @MockitoBean
    protected lateinit var getBalance: GetBalanceUseCase

    protected val accountId = AccountId.parse(DEFAULT_ACCOUNT_ID)
    protected val snapshot = BalanceSnapshot.from(transactionEvent())

    protected fun requestBalance(id: String = DEFAULT_ACCOUNT_ID): ResultActions = mockMvc.perform(get("/balances/{id}", id))

    protected fun problemTypeUri(slug: String) = "urn:problem-type:consulta-saldo:$slug"

    protected fun ResultActions.andExpectProblem(
        expectedStatus: HttpStatus,
        slug: String,
    ): ResultActions =
        andExpect(status().`is`(expectedStatus.value()))
            .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
            .andExpect(jsonPath("$.type").value(problemTypeUri(slug)))
            .andExpect(jsonPath("$.status").value(expectedStatus.value()))
            .andExpect(header().exists(CorrelationIdFilter.HEADER))

    protected fun MvcResult.header(name: String): String? = response.getHeader(name)
}
