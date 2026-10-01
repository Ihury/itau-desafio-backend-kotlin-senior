package br.com.itau.challenge

import com.lemonappdev.konsist.api.Konsist
import com.lemonappdev.konsist.api.architecture.KoArchitectureCreator.assertArchitecture
import com.lemonappdev.konsist.api.architecture.Layer
import com.lemonappdev.konsist.api.declaration.KoFileDeclaration
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

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

    private fun importsOutside(
        context: String,
        layer: String,
        allowedPrefixes: List<String>,
    ): List<String> =
        filesOf(context, layer).flatMap { file ->
            importsOf(file)
                .filter { import -> allowedPrefixes.none { import.startsWith(it) } }
                .map { "${file.path}: $it" }
        }

    private fun assertNoViolations(
        rule: String,
        violations: List<String>,
    ) = assertTrue(violations.isEmpty(), "$rule\n" + violations.joinToString("\n") { "  - $it" })

    @Test
    fun `hexagonal layers respect dependency direction in every context`() {
        contexts.forEach { context ->
            val present = HEXAGONAL_LAYERS.filter { filesOf(context, it).isNotEmpty() }.toSet()
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
                importsOutside(context, "domain", listOf("kotlin.", "java.", "$ROOT.$context.domain."))
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
                importsOutside(context, "application", allowed)
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
    fun `every context has the four hexagonal layers`() {
        val missing =
            contexts.flatMap { context ->
                HEXAGONAL_LAYERS.filter { filesOf(context, it).isEmpty() }.map { "contexto '$context' sem a camada '$it'" }
            }
        assertNoViolations("todo contexto de negocio precisa de $HEXAGONAL_LAYERS (config e o composition root, opcional)", missing)
    }

    @Test
    fun `at least one business context exists and balance has a domain of its own`() {
        assertTrue(contexts.isNotEmpty(), "nenhum contexto de negocio encontrado: as regras acima seriam vacuosas")
        assertTrue("balance" in contexts, "contexto 'balance' ausente; encontrados: $contexts")
        assertTrue(filesOf("balance", "domain").isNotEmpty(), "balance nao possui domain")
    }

    @Test
    fun `balance domain imports nothing beyond kotlin, java and itself`() {
        val forbidden =
            listOf(
                "org.springframework",
                "software.amazon",
                "org.apache.kafka",
                "tools.jackson",
                "com.fasterxml",
                "io.micrometer",
                "io.github.resilience4j",
                "org.slf4j",
            )
        val allowed = listOf("kotlin.", "java.", "$ROOT.balance.domain.")
        val violations =
            filesOf("balance", "domain").flatMap { file ->
                importsOf(file)
                    .filter { import -> forbidden.any { import.startsWith(it) } || allowed.none { import.startsWith(it) } }
                    .map { "${file.path}: $it" }
            }
        assertNoViolations("balance.domain so pode importar kotlin.*, java.* e o proprio dominio", violations)
    }

    @Test
    fun `no catch block in production swallows an exception without a log, a metric or a rethrow`() {
        val neverReturns = production.files.flatMap { NOTHING_FUNCTION.findAll(stripComments(it.text)).map { match -> match.groupValues[1] } }.toSet()
        val violations = production.files.flatMap { file -> silentCatches(file.text, neverReturns).map { "${file.path}: $it" } }
        assertNoViolations("todo catch em main precisa de log, metrica ou throw e nao pode ser vazio", violations)
    }

    @Test
    fun `the silent catch rule flags empty and swallowing catches and accepts log, metric, rethrow and never-returning helpers`() {
        fun flagged(body: String) = silentCatches("fun f() { try { g() } catch (e: Exception) { $body } }", setOf("giveUp")).size
        assertEquals(1, flagged(""), "catch vazio")
        assertEquals(1, flagged("// ignora"), "catch so com comentario")
        assertEquals(1, flagged("return null"), "catch que engole e devolve valor")
        assertEquals(1, flagged("fallback = true"), "catch que engole e segue")
        assertEquals(0, flagged("log.warn(\"x\", e)"), "com log")
        assertEquals(0, flagged("failures.increment()"), "com contador")
        assertEquals(0, flagged("durations.getValue(x).record(1, NANOSECONDS)"), "com timer")
        assertEquals(0, flagged("throw IllegalStateException()"), "relancando")
        assertEquals(0, flagged("giveUp(e)"), "helper que nunca retorna")
    }

    private fun silentCatches(
        source: String,
        neverReturns: Set<String>,
    ): List<String> {
        val text = stripComments(source)
        return CATCH.findAll(text).mapNotNull { match ->
            val open = text.indexOf('{', match.range.last)
            val body = if (open < 0) "" else blockAt(text, open).trim()
            val accepted =
                body.isNotEmpty() &&
                    (ACCOUNTED.containsMatchIn(body) || neverReturns.any { Regex("\\b${Regex.escape(it)}\\(").containsMatchIn(body) })
            if (accepted) null else "catch (${match.groupValues[1].trim()}) { ${body.take(60)} }"
        }.toList()
    }

    private fun blockAt(
        text: String,
        open: Int,
    ): String {
        var depth = 0
        for (index in open until text.length) {
            when (text[index]) {
                '{' -> depth++
                '}' -> if (--depth == 0) return text.substring(open + 1, index)
            }
        }
        return text.substring(open + 1)
    }

    private fun stripComments(source: String): String = source.replace(BLOCK_COMMENT, "").replace(LINE_COMMENT, "")

    private companion object {
        val CATCH = Regex("""catch\s*\(([^)]*)\)""")
        val BLOCK_COMMENT = Regex("""/\*.*?\*/""", RegexOption.DOT_MATCHES_ALL)
        val LINE_COMMENT = Regex("""(?m)^\s*//.*$|(?<=\s)//[^\n]*$""")
        val ACCOUNTED = Regex("""\bthrow\b|\blog\.\w+\(|\.increment\(|\brecord(?:Duration)?\(|\bmetrics\.""")
        val NOTHING_FUNCTION = Regex("""fun\s+(?:[\w<>?,. ]+\.)?(\w+)\([^)]*\)\s*:\s*Nothing""")
        const val ROOT = "br.com.itau.challenge"
        val HEXAGONAL_LAYERS = listOf("domain", "port", "application", "adapter")
        val KNOWN_LAYERS = setOf("domain", "port", "application", "adapter", "config")
        val ADAPTER_TECHNOLOGIES = listOf("input.web", "input.kafka", "output.dynamodb", "output.metrics")
    }
}
