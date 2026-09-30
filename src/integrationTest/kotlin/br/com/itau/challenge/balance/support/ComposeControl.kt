package br.com.itau.challenge.balance.support

import java.io.File
import java.io.IOException
import java.util.concurrent.TimeUnit

/**
 * Pausa/despausa servicos do docker compose deste projeto (testes de caos). Nunca para nem remove container, imagem ou volume.
 */
object ComposeControl {
    private val projectDir: File = File(System.getProperty("user.dir"))

    private data class CommandResult(
        val exitCode: Int,
        val output: String,
    )

    private fun runCompose(vararg args: String): CommandResult =
        try {
            val process =
                ProcessBuilder(listOf("docker", "compose") + args)
                    .directory(projectDir)
                    .redirectErrorStream(true)
                    .start()
            val finished = process.waitFor(60, TimeUnit.SECONDS)
            if (!finished) process.destroyForcibly()
            CommandResult(if (finished) process.exitValue() else -1, process.inputStream.bufferedReader().readText())
        } catch (failure: IOException) {
            CommandResult(-2, failure.message.orEmpty())
        }

    /** O Docker CLI existe e o servico esta em execucao (nao pausado) no projeto compose deste diretorio. */
    fun isRunning(service: String): Boolean {
        val outcome = runCompose("ps", "--status", "running", "--services")
        return outcome.exitCode == 0 && outcome.output.lines().any { it.trim() == service }
    }

    fun pause(service: String) {
        val outcome = runCompose("pause", service)
        check(outcome.exitCode == 0) { "docker compose pause $service falhou: ${outcome.output}" }
    }

    /** Idempotente: um `unpause` de servico que nao esta pausado e ignorado (deve ser sempre seguro chamar em `finally`). */
    fun unpause(service: String) {
        runCompose("unpause", service)
    }

    /** Detecta `pause` esquecido por uma execucao anterior. */
    fun isPaused(service: String): Boolean {
        val outcome = runCompose("ps", "--status", "paused", "--services")
        return outcome.output.lines().any { it.trim() == service }
    }
}
