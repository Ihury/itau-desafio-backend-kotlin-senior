package br.com.itau.challenge.hello.application

import br.com.itau.challenge.hello.domain.exception.BlankRequesterNameException
import br.com.itau.challenge.hello.port.output.GreetingTemplateProvider
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class GreetingServiceTest {

    @Test
    fun `should apply requester name to the selected template`() {
        val templateProvider = GreetingTemplateProvider { "Hello, %s!" }
        val service = GreetingService(templateProvider)

        val greeting = service.getGreeting("Ada")

        assertEquals("Hello, Ada!", greeting.message)
    }

    @Test
    fun `should trim surrounding whitespace from the requester name`() {
        val templateProvider = GreetingTemplateProvider { "Hello, %s!" }
        val service = GreetingService(templateProvider)

        val greeting = service.getGreeting("  Ada  ")

        assertEquals("Hello, Ada!", greeting.message)
    }

    @Test
    fun `should preserve internal whitespace of multi-word names`() {
        val templateProvider = GreetingTemplateProvider { "Welcome, %s!" }
        val service = GreetingService(templateProvider)

        val greeting = service.getGreeting("Ada Lovelace")

        assertEquals("Welcome, Ada Lovelace!", greeting.message)
    }

    @Test
    fun `should accept names containing punctuation`() {
        val templateProvider = GreetingTemplateProvider { "Hello, %s!" }
        val service = GreetingService(templateProvider)

        val greeting = service.getGreeting("O'Connor")

        assertEquals("Hello, O'Connor!", greeting.message)
    }

    @Test
    fun `should format using whichever template the provider returns`() {
        val templateProvider = GreetingTemplateProvider { "Hey %s, great to see you!" }
        val service = GreetingService(templateProvider)

        val greeting = service.getGreeting("Grace")

        assertEquals("Hey Grace, great to see you!", greeting.message)
    }

    @Test
    fun `should reject a blank requester name`() {
        val templateProvider = GreetingTemplateProvider { "Hello, %s!" }
        val service = GreetingService(templateProvider)

        assertFailsWith<BlankRequesterNameException> { service.getGreeting("   ") }
    }

    @Test
    fun `should reject an empty requester name`() {
        val templateProvider = GreetingTemplateProvider { "Hello, %s!" }
        val service = GreetingService(templateProvider)

        assertFailsWith<BlankRequesterNameException> { service.getGreeting("") }
    }
}
