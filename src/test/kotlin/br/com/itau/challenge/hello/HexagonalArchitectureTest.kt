package br.com.itau.challenge.hello

import com.lemonappdev.konsist.api.Konsist
import com.lemonappdev.konsist.api.architecture.KoArchitectureCreator.assertArchitecture
import com.lemonappdev.konsist.api.architecture.Layer
import com.lemonappdev.konsist.api.verify.assertFalse
import org.junit.jupiter.api.Test

class HexagonalArchitectureTest {

    private val scope = Konsist.scopeFromPackage("br.com.itau.challenge.hello..")

    private val domain = Layer("Domain", "..hello.domain..")
    private val port = Layer("Port", "..hello.port..")
    private val application = Layer("Application", "..hello.application..")
    private val adapter = Layer("Adapter", "..hello.adapter..")

    @Test
    fun `hexagonal layers respect dependency direction`() {
        scope.assertArchitecture {
            domain.dependsOnNothing()
            port.doesNotDependOn(application, adapter)
            application.doesNotDependOn(adapter)
        }
    }

    @Test
    fun `domain does not depend on the Spring framework`() {
        Konsist
            .scopeFromPackage("br.com.itau.challenge.hello.domain..")
            .files
            .assertFalse { it.hasImport { import -> import.name.startsWith("org.springframework") } }
    }
}
