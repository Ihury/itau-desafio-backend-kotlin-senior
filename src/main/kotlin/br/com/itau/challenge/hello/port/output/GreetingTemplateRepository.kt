package br.com.itau.challenge.hello.port.output

import br.com.itau.challenge.hello.domain.model.GreetingTemplate

fun interface GreetingTemplateRepository {
    fun save(greetingTemplate: GreetingTemplate)
}
