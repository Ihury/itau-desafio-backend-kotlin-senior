package br.com.itau.challenge.hello.port.input

import br.com.itau.challenge.hello.domain.model.GreetingTemplate

fun interface SaveGreetingTemplateUseCase {
    fun saveGreetingTemplate(greetingTemplate: GreetingTemplate)
}
