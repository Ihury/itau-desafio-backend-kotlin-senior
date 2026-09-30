package br.com.itau.challenge.balance.support

import java.io.File
import java.util.concurrent.TimeUnit

/**
 * Controle do docker compose DESTE projeto para os testes de caos (`docker compose pause|unpause dynamodb`). So age sobre o
 * servico informado do projeto compose do diretorio corrente; nunca para nem remove container, imagem ou volume.
 */
object ComposeControl {
    private val projectDir: File = File(System.getProperty("user.dir"))

    private data class Outcome(
        val exitCode: Int,
        val output: String,
    )

    private fun run(vararg args: String): Outcome =
        try {
            val process =
                ProcessBuilder(listOf("docker", "compose") + args)
                    .directory(projectDir)
                    .redirectErrorStream(true)
                    .start()
            val finished = process.waitFor(60, TimeUnit.SECONDS)
            if (!finished) process.destroyForcibly()
            Outcome(if (finished) process.exitValue() else -1, process.inputStream.bufferedReader().readText())
        } catch (failure: java.io.IOException) {
            Outcome(-2, failure.message.orEmpty())
        }

    /** O Docker CLI existe e o servico esta em execucao (nao pausado) no projeto compose deste diretorio. */
    fun isRunning(service: String): Boolean {
        val outcome = run("ps", "--status", "running", "--services")
        return outcome.exitCode == 0 && outcome.output.lines().any { it.trim() == service }
    }

    fun pause(service: String) {
        val outcome = run("pause", service)
        check(outcome.exitCode == 0) { "docker compose pause $service falhou: ${outcome.output}" }
    }

    /** Idempotente: um `unpause` de servico que nao esta pausado e ignorado (deve ser sempre seguro chamar em `finally`). */
    fun unpause(service: String) {
        run("unpause", service)
    }

    /** Estado `paused` do servico (para provar que ficou ativo ao fim do teste). */
    fun isPaused(service: String): Boolean {
        val outcome = run("ps", "--status", "paused", "--services")
        return outcome.output.lines().any { it.trim() == service }
    }
}
