package br.com.itau.challenge.hello.adapter.input.kafka

import br.com.itau.challenge.hello.domain.model.GreetingTemplate
import br.com.itau.challenge.hello.port.input.SaveGreetingTemplateUseCase
import tools.jackson.databind.json.JsonMapper
import kotlin.test.Test
import kotlin.test.assertEquals

class GreetingTemplateConsumerTest {

    private val objectMapper = JsonMapper.builder().build()

    @Test
    fun `should deserialize the JSON payload and delegate it to the save use case`() {
        val savedTemplates = mutableListOf<GreetingTemplate>()
        val useCase = SaveGreetingTemplateUseCase { savedTemplates.add(it) }
        val consumer = GreetingTemplateConsumer(useCase, objectMapper)

        consumer.consume("""{"id": "k1", "template": "Yo %s!"}""")

        assertEquals(listOf(GreetingTemplate("k1", "Yo %s!")), savedTemplates)
    }
}
