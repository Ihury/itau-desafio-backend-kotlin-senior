package br.com.itau.challenge.balance.support

import java.io.File
import java.io.IOException
import java.util.concurrent.TimeUnit

// So pause/unpause: nunca parar nem remover container, imagem ou volume.
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

    fun isRunning(service: String): Boolean {
        val outcome = runCompose("ps", "--status", "running", "--services")
        return outcome.exitCode == 0 && outcome.output.lines().any { it.trim() == service }
    }

    fun pause(service: String) {
        val outcome = runCompose("pause", service)
        check(outcome.exitCode == 0) { "docker compose pause $service falhou: ${outcome.output}" }
    }

    fun unpauseIgnoringFailure(service: String) {
        runCompose("unpause", service)
    }

    fun isLeftPaused(service: String): Boolean {
        val outcome = runCompose("ps", "--status", "paused", "--services")
        return outcome.output.lines().any { it.trim() == service }
    }
}
