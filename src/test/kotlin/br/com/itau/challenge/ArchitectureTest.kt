package br.com.itau.challenge

import com.lemonappdev.konsist.api.Konsist
import com.lemonappdev.konsist.api.architecture.KoArchitectureCreator.assertArchitecture
import com.lemonappdev.konsist.api.architecture.Layer
import com.lemonappdev.konsist.api.declaration.KoFileDeclaration
import org.junit.jupiter.api.Test
import kotlin.test.assertTrue

/**
 * Verificacao da arquitetura hexagonal para TODOS os contextos de negocio (Constitution I).
 *
 * Os contextos sao descobertos como subpacotes diretos de `br.com.itau.challenge`; um contexto novo
 * entra na verificacao automaticamente. Camadas ainda vazias sao toleradas (nenhuma regra e vacuosa
 * por acidente: o teste de existencia de contexto entra junto com o primeiro codigo de negocio).
 */
class ArchitectureTest {

    private val production = Konsist.scopeFromProduction()

    private val contexts: List<String> =
        production.files
            .mapNotNull { it.packagee?.name }
            .filter { it.startsWith("$ROOT.") }
            .map { it.removePrefix("$ROOT.").substringBefore('.') }
            .distinct()
            .sorted()

    private fun filesOf(
        context: String,
        layer: String,
    ): List<KoFileDeclaration> = production.files.filter { inPackage(it, "$ROOT.$context.$layer") }

    private fun inPackage(
        file: KoFileDeclaration,
        base: String,
    ): Boolean = file.packagee?.name.let { it == base || it?.startsWith("$base.") == true }

    private fun importsOf(file: KoFileDeclaration): List<String> = file.imports.map { it.name }

    private fun assertNoViolations(
        rule: String,
        violations: List<String>,
    ) = assertTrue(violations.isEmpty(), "$rule\n" + violations.joinToString("\n") { "  - $it" })

    @Test
    fun `hexagonal layers respect dependency direction in every context`() {
        contexts.forEach { context ->
            val present = LAYERS_WITH_DIRECTION.filter { filesOf(context, it).isNotEmpty() }.toSet()
            val layer = present.associateWith { Layer(it, "$ROOT.$context.$it..") }
            val domain = layer["domain"]
            val port = layer["port"]
            val application = layer["application"]
            val adapter = layer["adapter"]

            Konsist.scopeFromPackage("$ROOT.$context..").assertArchitecture {
                if (domain != null) domain.dependsOnNothing()
                if (port != null && application != null && adapter != null) port.doesNotDependOn(application, adapter)
                if (port != null && application != null && adapter == null) port.doesNotDependOn(application)
                if (port != null && application == null && adapter != null) port.doesNotDependOn(adapter)
                if (application != null && adapter != null) application.doesNotDependOn(adapter)
            }
        }
    }

    @Test
    fun `domain imports only kotlin, java and its own package`() {
        val violations =
            contexts.flatMap { context ->
                val allowed = listOf("kotlin.", "java.", "$ROOT.$context.domain.")
                filesOf(context, "domain").flatMap { file ->
                    importsOf(file)
                        .filter { import -> allowed.none { import.startsWith(it) } }
                        .map { "${file.path}: $it" }
                }
            }
        assertNoViolations("domain deve ser Kotlin puro (kotlin.*, java.* e o proprio dominio)", violations)
    }

    @Test
    fun `application imports only domain, port, Service stereotype and slf4j`() {
        val violations =
            contexts.flatMap { context ->
                val allowed =
                    listOf(
                        "kotlin.",
                        "java.",
                        "$ROOT.$context.domain.",
                        "$ROOT.$context.port.",
                        "$ROOT.$context.application.",
                        "org.springframework.stereotype.Service",
                        "org.slf4j.",
                    )
                filesOf(context, "application").flatMap { file ->
                    importsOf(file)
                        .filter { import -> allowed.none { import.startsWith(it) } }
                        .map { "${file.path}: $it" }
                }
            }
        assertNoViolations("application so depende de domain, port, @Service e slf4j", violations)
    }

    @Test
    fun `technology adapters do not import one another`() {
        val violations =
            contexts.flatMap { context ->
                ADAPTER_TECHNOLOGIES.flatMap { technology ->
                    val others = ADAPTER_TECHNOLOGIES - technology
                    filesOf(context, "adapter.$technology").flatMap { file ->
                        importsOf(file)
                            .filter { import -> others.any { import.startsWith("$ROOT.$context.adapter.$it.") } }
                            .map { "${file.path}: $it" }
                    }
                }
            }
        assertNoViolations("adapters de tecnologias diferentes nao se importam", violations)
    }

    @Test
    fun `nothing outside config depends on config`() {
        val configImport = Regex("^${Regex.escape(ROOT)}\\.[^.]+\\.config(\\..*)?$")
        val violations =
            production.files
                .filter { file -> file.packagee?.name?.split('.')?.contains("config") != true }
                .flatMap { file -> importsOf(file).filter { configImport.matches(it) }.map { "${file.path}: $it" } }
        assertNoViolations("apenas config (composition root) pode ser referenciado por config", violations)
    }

    @Test
    fun `every direct subpackage of the root is a context made only of known layers`() {
        val violations =
            production.files.mapNotNull { file ->
                val pkg = file.packagee?.name ?: return@mapNotNull null
                val segments = pkg.removePrefix(ROOT).removePrefix(".").split('.').filter { it.isNotEmpty() }
                val layer = segments.getOrNull(1)
                if (layer != null && layer !in KNOWN_LAYERS) "${file.path}: subpacote '$layer' desconhecido no contexto '${segments[0]}'" else null
            }
        assertNoViolations("contextos so podem ter as camadas $KNOWN_LAYERS", violations)
    }

    @Test
    fun `at least one business context exists and balance has a domain of its own`() {
        assertTrue(contexts.isNotEmpty(), "nenhum contexto de negocio encontrado: as regras acima seriam vacuosas")
        assertTrue("balance" in contexts, "contexto 'balance' ausente; encontrados: $contexts")
        assertTrue(filesOf("balance", "domain").isNotEmpty(), "balance nao possui domain")
    }

    @Test
    fun `balance domain imports nothing beyond kotlin, java and itself`() {
        val forbidden = listOf("org.springframework", "software.amazon", "org.apache.kafka", "tools.jackson", "com.fasterxml", "io.micrometer", "io.github.resilience4j", "org.slf4j")
        val violations =
            filesOf("balance", "domain").flatMap { file ->
                importsOf(file)
                    .filter { import -> forbidden.any { import.startsWith(it) } || !(import.startsWith("kotlin.") || import.startsWith("java.") || import.startsWith("$ROOT.balance.domain.")) }
                    .map { "${file.path}: $it" }
            }
        assertNoViolations("balance.domain so pode importar kotlin.*, java.* e o proprio dominio", violations)
    }

    private companion object {
        const val ROOT = "br.com.itau.challenge"
        val LAYERS_WITH_DIRECTION = listOf("domain", "port", "application", "adapter")
        val KNOWN_LAYERS = setOf("domain", "port", "application", "adapter", "config")
        val ADAPTER_TECHNOLOGIES = listOf("input.web", "input.kafka", "output.dynamodb", "output.metrics")
    }
}
