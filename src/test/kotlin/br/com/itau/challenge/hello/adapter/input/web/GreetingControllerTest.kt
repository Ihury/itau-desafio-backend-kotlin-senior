package br.com.itau.challenge.hello.adapter.input.web

import br.com.itau.challenge.hello.port.output.GreetingTemplateProvider
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.BDDMockito.given
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import org.springframework.test.web.servlet.post

@SpringBootTest
@AutoConfigureMockMvc
class GreetingControllerTest(
    @Autowired private val mockMvc: MockMvc,
) {

    @MockitoBean
    private lateinit var greetingTemplateProvider: GreetingTemplateProvider

    @BeforeEach
    fun stubTemplateProvider() {
        given(greetingTemplateProvider.randomTemplate()).willReturn("Hello, %s!")
    }

    @Test
    fun `should return a JSON greeting containing the requester name`() {
        mockMvc.get("/hello") {
            param("name", "Ada")
        }.andExpect {
            status { isOk() }
            content { contentType(MediaType.APPLICATION_JSON) }
            jsonPath("$.message") { value(containsString("Ada")) }
        }
    }

    @Test
    fun `should support unicode characters in the requester name`() {
        mockMvc.get("/hello") {
            param("name", "José")
        }.andExpect {
            status { isOk() }
            content { contentType(MediaType.APPLICATION_JSON) }
            jsonPath("$.message") { value(containsString("José")) }
        }
    }

    @Test
    fun `should reject unsupported HTTP methods`() {
        mockMvc.post("/hello") {
            param("name", "Ada")
        }.andExpect {
            status { isMethodNotAllowed() }
        }
    }
}
