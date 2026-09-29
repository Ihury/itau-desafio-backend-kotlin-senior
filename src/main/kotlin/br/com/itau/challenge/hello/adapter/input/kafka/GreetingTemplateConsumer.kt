package br.com.itau.challenge.hello.adapter.input.kafka

import br.com.itau.challenge.hello.adapter.input.kafka.dto.GreetingTemplateMessage
import br.com.itau.challenge.hello.domain.model.GreetingTemplate
import br.com.itau.challenge.hello.port.input.SaveGreetingTemplateUseCase
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

@Component
class GreetingTemplateConsumer(
    private val saveGreetingTemplateUseCase: SaveGreetingTemplateUseCase,
    private val objectMapper: ObjectMapper,
) {

    @KafkaListener(topics = ["\${greeting-templates.topic-name}"])
    fun consume(payload: String) {
        val message = objectMapper.readValue(payload, GreetingTemplateMessage::class.java)
        saveGreetingTemplateUseCase.saveGreetingTemplate(
            GreetingTemplate(id = message.id, template = message.template),
        )
    }
}
