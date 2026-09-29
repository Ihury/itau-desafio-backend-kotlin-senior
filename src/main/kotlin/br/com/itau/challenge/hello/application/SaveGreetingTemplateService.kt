package br.com.itau.challenge.hello.application

import br.com.itau.challenge.hello.domain.exception.InvalidGreetingTemplateException
import br.com.itau.challenge.hello.domain.model.GreetingTemplate
import br.com.itau.challenge.hello.port.input.SaveGreetingTemplateUseCase
import br.com.itau.challenge.hello.port.output.GreetingTemplateRepository
import org.springframework.stereotype.Service

@Service
class SaveGreetingTemplateService(
    private val greetingTemplateRepository: GreetingTemplateRepository,
) : SaveGreetingTemplateUseCase {

    override fun saveGreetingTemplate(greetingTemplate: GreetingTemplate) {
        if (greetingTemplate.id.isBlank()) {
            throw InvalidGreetingTemplateException("Greeting template id must not be blank")
        }
        if (greetingTemplate.template.isBlank()) {
            throw InvalidGreetingTemplateException("Greeting template must not be blank")
        }

        greetingTemplateRepository.save(greetingTemplate)
    }
}
