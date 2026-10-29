// Every module: Java 17 bytecode, Error Prone with NullAway, SpotBugs, Checkstyle, and the shared test kit.

import com.github.spotbugs.snom.Effort
import com.github.spotbugs.snom.SpotBugsTask
import net.ltgt.gradle.errorprone.errorprone
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    `java-library`
    checkstyle
    idea
    id("net.ltgt.errorprone")
    id("com.github.spotbugs")
}

val envoyXds = extensions.create<EnvoyXdsExtension>("envoyXds")

group = "net.woaf.jono.envoy"

val libs = versionCatalogs.named("libs")

dependencies {
    testImplementation(platform(libs.findLibrary("junit-bom").get()))
    testImplementation(libs.findLibrary("junit-jupiter").get())
    testRuntimeOnly(libs.findLibrary("junit-platform-launcher").get())
    testImplementation(libs.findLibrary("assertj").get())
    testImplementation(libs.findLibrary("mockito-core").get())
    testImplementation(libs.findLibrary("mockito-junit-jupiter").get())
    spotbugs(libs.findLibrary("spotbugs").get())
    errorprone(libs.findLibrary("errorprone-core").get())
    errorprone(libs.findLibrary("nullaway").get())
    api(libs.findLibrary("jspecify").get())
}

java {
    withSourcesJar()
    withJavadocJar()
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Title" to envoyXds.displayName,
            "Implementation-Version" to archiveVersion,
        )
    }
}

tasks.withType<JavaCompile>().configureEach {
    // Error Prone needs JDK 21+ to run; keep the published bytecode at 17.
    options.release = 17
    options.errorprone.disableWarningsInGeneratedCode = true
    options.compilerArgs.add("-Werror")
}

tasks.compileJava {
    options.encoding = "utf-8"
    options.compilerArgs.addAll(listOf("-parameters", "-Xlint", "-Xlint:-processing"))
    // Every main package is @NullMarked (see its package-info.java), and NullAway checks it strictly.
    options.errorprone {
        error("NullAway")
        option("NullAway:OnlyNullMarked", "true")
        option("NullAway:JSpecifyMode", "true")
        option("NullAway:CheckOptionalEmptiness", "true")
        option("NullAway:AcknowledgeRestrictiveAnnotations", "true")
        // Off by default in Error Prone, but each found something here.
        error("NonFinalStaticField", "RedundantNullCheck", "ThrowSpecificExceptions")
    }
}

tasks.compileTestJava {
    // Hegel calls its native engine through the FFM API. Tests aren't published, so needn't run on 17.
    options.release = 22
    // Tests aren't @NullMarked, and often call a method for its side effect alone.
    options.errorprone.disable("NullAway", "CheckReturnValue")
}

tasks.javadoc {
    (options as StandardJavadocDocletOptions).addBooleanOption("html5", true)
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--enable-native-access=ALL-UNNAMED") // Hegel
    testLogging {
        events(TestLogEvent.STARTED, TestLogEvent.FAILED, TestLogEvent.SKIPPED, TestLogEvent.PASSED, TestLogEvent.STANDARD_ERROR)
        showExceptions = true
        showStackTraces = true
        exceptionFormat = TestExceptionFormat.FULL
    }
}

tasks.withType<Checkstyle>().configureEach {
    config = resources.text.fromFile(rootProject.file("google_checks.xml"))
    maxWarnings = 0
    reports {
        xml.required = true
        html.required = false
    }
}

tasks.withType<SpotBugsTask>().configureEach {
    effort = Effort.MAX
    maxHeapSize = "1g"
    includeFilter = rootProject.file("spotbugs_include_filter.xml")
    omitVisitors = listOf("FindReturnRef", "ThrowingExceptions")
    reports.create("xml") {
        required = true
    }
}
