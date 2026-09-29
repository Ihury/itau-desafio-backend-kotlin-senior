package br.com.itau.challenge.balance.adapter.input.web

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/** O contrato e servido em `GET /openapi.yaml` com o mesmo conteudo do arquivo do classpath. */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class OpenApiServedTest {
    @Autowired
    private lateinit var mockMvc: MockMvc

    @Test
    fun `openapi yaml is served with the same content as the classpath copy and a yaml media type`() {
        val expected = assertNotNull(javaClass.getResourceAsStream("/static/openapi.yaml")).use { it.readAllBytes() }

        val response = mockMvc.perform(get("/openapi.yaml")).andExpect(status().isOk).andReturn().response

        assertTrue(expected.contentEquals(response.contentAsByteArray))
        assertTrue(response.contentType!!.contains("yaml"), "content type: ${response.contentType}")
        assertEquals(true, response.contentAsString.startsWith("openapi: 3.1.0"))
    }
}
