package br.com.itau.challenge.balance.support

data class PrometheusSample(
    val name: String,
    val labels: Map<String, String>,
    val value: Double,
) {
    companion object {
        private val LINE = Regex("""^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)})?\s+(\S+)(?:\s+\d+)?$""")
        private val LABEL = Regex("""([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"""")

        fun parse(text: String): List<PrometheusSample> =
            text
                .lineSequence()
                .filter { it.isNotBlank() && !it.startsWith("#") }
                .mapNotNull { line ->
                    val match = LINE.matchEntire(line) ?: return@mapNotNull null
                    val labels = LABEL.findAll(match.groupValues[2]).associate { it.groupValues[1] to it.groupValues[2] }
                    val value = match.groupValues[3].let { if (it == "+Inf") Double.POSITIVE_INFINITY else it.toDoubleOrNull() }
                    value?.let { PrometheusSample(match.groupValues[1], labels, it) }
                }.toList()
    }
}

fun List<PrometheusSample>.sumOfSamples(
    name: String,
    vararg labels: Pair<String, String>,
): Double = filter { it.name == name && labels.all { (key, value) -> it.labels[key] == value } }.sumOf { it.value }

fun List<PrometheusSample>.singleValue(
    name: String,
    vararg labels: Pair<String, String>,
): Double =
    filter { it.name == name && labels.all { (key, value) -> it.labels[key] == value } }
        .also { check(it.size == 1) { "esperava 1 amostra de $name $labels, achei ${it.size}" } }
        .single()
        .value

fun List<PrometheusSample>.histogramQuantile(
    q: Double,
    metric: String,
    vararg labels: Pair<String, String>,
): Double {
    val buckets =
        filter { it.name == "${metric}_bucket" && labels.all { (key, value) -> it.labels[key] == value } }
            .groupBy { it.labels.getValue("le").let { le -> if (le == "+Inf") Double.POSITIVE_INFINITY else le.toDouble() } }
            .mapValues { (_, samples) -> samples.sumOf { it.value } }
            .toSortedMap()
    check(buckets.isNotEmpty()) { "sem buckets de $metric" }
    val total = buckets.values.last()
    if (total == 0.0) return Double.NaN
    val rank = q * total
    var previousBound = 0.0
    var previousCount = 0.0
    for ((bound, count) in buckets) {
        if (count >= rank) {
            if (bound.isInfinite()) return previousBound
            return previousBound + (bound - previousBound) * ((rank - previousCount) / (count - previousCount))
        }
        previousBound = bound
        previousCount = count
    }
    return previousBound
}
