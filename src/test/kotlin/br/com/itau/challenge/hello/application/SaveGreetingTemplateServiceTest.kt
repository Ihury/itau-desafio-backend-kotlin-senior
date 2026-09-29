package br.com.itau.challenge.hello.application

import br.com.itau.challenge.hello.domain.exception.InvalidGreetingTemplateException
import br.com.itau.challenge.hello.domain.model.GreetingTemplate
import br.com.itau.challenge.hello.port.output.GreetingTemplateRepository
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class SaveGreetingTemplateServiceTest {

    @Test
    fun `should save a valid greeting template`() {
        val savedTemplates = mutableListOf<GreetingTemplate>()
        val repository = GreetingTemplateRepository { savedTemplates.add(it) }
        val service = SaveGreetingTemplateService(repository)

        service.saveGreetingTemplate(GreetingTemplate(id = "k1", template = "Yo %s!"))

        assertEquals(listOf(GreetingTemplate("k1", "Yo %s!")), savedTemplates)
    }

    @Test
    fun `should reject a blank id`() {
        val repository = GreetingTemplateRepository { }
        val service = SaveGreetingTemplateService(repository)

        assertFailsWith<InvalidGreetingTemplateException> {
            service.saveGreetingTemplate(GreetingTemplate(id = "  ", template = "Yo %s!"))
        }
    }

    @Test
    fun `should reject a blank template`() {
        val repository = GreetingTemplateRepository { }
        val service = SaveGreetingTemplateService(repository)

        assertFailsWith<InvalidGreetingTemplateException> {
            service.saveGreetingTemplate(GreetingTemplate(id = "k1", template = "   "))
        }
    }
}
