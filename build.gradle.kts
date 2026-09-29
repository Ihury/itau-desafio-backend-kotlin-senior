plugins {
	kotlin("jvm") version "2.3.21"
	kotlin("plugin.spring") version "2.3.21"
	id("org.springframework.boot") version "4.1.0"
	id("io.spring.dependency-management") version "1.1.7"
	jacoco
}

group = "br.com.itau"
version = "0.0.1-SNAPSHOT"
description = "itau-code-challange-starter-kit"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(21)
	}
}

repositories {
	mavenCentral()
}

dependencies {
	implementation(platform("software.amazon.awssdk:bom:2.46.7"))
	implementation("org.springframework.boot:spring-boot-starter-webmvc")
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	implementation("tools.jackson.module:jackson-module-kotlin")
	implementation("software.amazon.awssdk:dynamodb")
	implementation("org.springframework.boot:spring-boot-starter-kafka")
	testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
	testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
	testImplementation("com.lemonappdev:konsist:0.17.3")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
	compilerOptions {
		freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
	}
}

// Integration tests require live infrastructure (e.g. `make db-up`) and are intentionally
// kept out of `test`/`check` so the default build stays infra-free and gate-friendly.
sourceSets {
	create("integrationTest") {
		kotlin.srcDir("src/integrationTest/kotlin")
		resources.srcDir("src/integrationTest/resources")
		compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
		runtimeClasspath += sourceSets.main.get().output + sourceSets.test.get().output
	}
}

configurations["integrationTestImplementation"].extendsFrom(configurations.testImplementation.get())
configurations["integrationTestRuntimeOnly"].extendsFrom(configurations.testRuntimeOnly.get())

val integrationTest =
	tasks.register<Test>("integrationTest") {
		description = "Runs integration tests against live infrastructure (start it first with `make db-up`)."
		group = "verification"
		testClassesDirs = sourceSets["integrationTest"].output.classesDirs
		classpath = sourceSets["integrationTest"].runtimeClasspath
		useJUnitPlatform()
		shouldRunAfter(tasks.test)

		testLogging {
			events("passed", "skipped", "failed")
			exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.SHORT
		}
	}

jacoco {
	toolVersion = "0.8.12"
}

tasks.withType<Test> {
	useJUnitPlatform()
	finalizedBy(tasks.jacocoTestReport)

	testLogging {
		events("passed", "skipped", "failed")
		exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.SHORT
		showStandardStreams = false
	}

	addTestListener(
		object : org.gradle.api.tasks.testing.TestListener {
			override fun beforeSuite(suite: org.gradle.api.tasks.testing.TestDescriptor) = Unit

			override fun beforeTest(testDescriptor: org.gradle.api.tasks.testing.TestDescriptor) = Unit

			override fun afterTest(
				testDescriptor: org.gradle.api.tasks.testing.TestDescriptor,
				result: org.gradle.api.tasks.testing.TestResult,
			) = Unit

			override fun afterSuite(
				suite: org.gradle.api.tasks.testing.TestDescriptor,
				result: org.gradle.api.tasks.testing.TestResult,
			) {
				if (suite.className != null) {
					val outcome = if (result.failedTestCount == 0L) "PASSED" else "FAILED"
					println(
						"  %-70s %-6s (%d tests, %d passed, %d failed, %d skipped)".format(
							suite.className,
							outcome,
							result.testCount,
							result.successfulTestCount,
							result.failedTestCount,
							result.skippedTestCount,
						),
					)
				}
			}
		},
	)
}

val jacocoCoverageExclusions =
	listOf(
		// Framework bootstrap: `main` is never invoked by tests, only Spring's test context machinery.
		"br/com/itau/challenge/ApplicationKt.class",
		"br/com/itau/challenge/Application.class",
	)

val coverageMinimum = 0.90

tasks.jacocoTestReport {
	dependsOn(tasks.test)

	classDirectories.setFrom(
		classDirectories.files.map {
			fileTree(it) { exclude(jacocoCoverageExclusions) }
		},
	)

	reports {
		xml.required = true
		html.required = true
	}

	doLast {
		printCoverageSummary(reports.xml.outputLocation.asFile.get(), coverageMinimum)
	}
}

tasks.jacocoTestCoverageVerification {
	dependsOn(tasks.jacocoTestReport)

	classDirectories.setFrom(
		classDirectories.files.map {
			fileTree(it) { exclude(jacocoCoverageExclusions) }
		},
	)

	violationRules {
		rule {
			limit {
				minimum = coverageMinimum.toBigDecimal()
			}
		}
	}
}

tasks.check {
	dependsOn(tasks.jacocoTestCoverageVerification)
}

fun printCoverageSummary(
	reportFile: File,
	minimum: Double,
) {
	if (!reportFile.exists()) return

	val factory = javax.xml.parsers.DocumentBuilderFactory.newInstance()
	factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false)
	factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false)
	val document = factory.newDocumentBuilder().parse(reportFile)
	val root = document.documentElement

	fun directCounters(element: org.w3c.dom.Element): Map<String, Pair<Int, Int>> {
		val counters = mutableMapOf<String, Pair<Int, Int>>()
		val children = element.childNodes
		for (i in 0 until children.length) {
			val node = children.item(i)
			if (node is org.w3c.dom.Element && node.tagName == "counter") {
				val covered = node.getAttribute("covered").toInt()
				val missed = node.getAttribute("missed").toInt()
				counters[node.getAttribute("type")] = covered to missed
			}
		}
		return counters
	}

	fun percentage(
		counters: Map<String, Pair<Int, Int>>,
		type: String,
	): Double {
		val (covered, missed) = counters[type] ?: return 100.0
		val total = covered + missed
		return if (total == 0) 100.0 else covered.toDouble() / total * 100.0
	}

	val overall = directCounters(root)
	val types = listOf("INSTRUCTION", "BRANCH", "LINE", "COMPLEXITY", "METHOD", "CLASS")

	val header = "%-12s %8s %8s %8s %8s".format("Type", "Covered", "Missed", "Total", "Coverage")
	val bar = "-".repeat(header.length)

	println()
	println("Coverage summary")
	println(bar)
	println(header)
	println(bar)

	for (type in types) {
		val (covered, missed) = overall[type] ?: continue
		val total = covered + missed
		println("%-12s %8d %8d %8d %7.1f%%".format(type.lowercase().replaceFirstChar { it.uppercase() }, covered, missed, total, percentage(overall, type)))
	}

	println(bar)

	val instructionPct = percentage(overall, "INSTRUCTION")
	val gate = if (instructionPct >= minimum * 100) "PASS" else "FAIL"
	println("Gate: minimum %.0f%% instruction coverage -> %s (%.1f%%)".format(minimum * 100, gate, instructionPct))
	println()
}
