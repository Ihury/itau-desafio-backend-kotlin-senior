package br.com.itau.challenge.hello.application

import br.com.itau.challenge.hello.domain.exception.BlankRequesterNameException
import br.com.itau.challenge.hello.domain.model.Greeting
import br.com.itau.challenge.hello.port.input.GetGreetingUseCase
import br.com.itau.challenge.hello.port.output.GreetingTemplateProvider
import org.springframework.stereotype.Service

@Service
class GreetingService(
    private val greetingTemplateProvider: GreetingTemplateProvider,
) : GetGreetingUseCase {

    override fun getGreeting(requesterName: String): Greeting {
        if (requesterName.isBlank()) {
            throw BlankRequesterNameException()
        }

        val template = greetingTemplateProvider.randomTemplate()
        return Greeting(message = template.format(requesterName.trim()))
    }
}
