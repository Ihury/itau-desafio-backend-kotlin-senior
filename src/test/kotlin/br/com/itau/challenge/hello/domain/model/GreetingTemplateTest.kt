package br.com.itau.challenge.hello.domain.model

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class GreetingTemplateTest {

    @Test
    fun `should expose the id and template it was created with`() {
        val greetingTemplate = GreetingTemplate(id = "k1", template = "Yo %s!")

        assertEquals("k1", greetingTemplate.id)
        assertEquals("Yo %s!", greetingTemplate.template)
    }

    @Test
    fun `should be equal when id and template are equal`() {
        assertEquals(GreetingTemplate("k1", "Yo %s!"), GreetingTemplate("k1", "Yo %s!"))
    }

    @Test
    fun `should not be equal when template differs`() {
        assertNotEquals(GreetingTemplate("k1", "Yo %s!"), GreetingTemplate("k1", "Sup %s?"))
    }
}
