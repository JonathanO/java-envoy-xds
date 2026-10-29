import net.ltgt.gradle.errorprone.errorprone

plugins {
    id("envoy-xds.published")
    id("envoy-xds.mutation-tested")
    alias(libs.plugins.fray)
    alias(libs.plugins.jmh)
}

description = "Snapshot-style config for Envoy xDS, with automatic make-before-break ordering"
envoyXds.displayName = "Envoy xDS Snapshot"

// ./gradlew :envoy-xds-snapshot:frayTest explores thread interleavings of the @FrayTest tests on Fray's instrumented
// JDK. It takes minutes, so it isn't part of check: CI runs it as a job of its own.

// ./gradlew :envoy-xds-snapshot:jmh runs the benchmarks in src/jmh; not part of the build.
jmh {
    jmhVersion = "1.37"
    fork = 1
    warmupIterations = 3
    warmup = "2s"
    iterations = 5
    timeOnIteration = "2s"
}

tasks.named<JavaCompile>("compileJmhJava") {
    options.errorprone.disable("NullAway") // benchmarks aren't @NullMarked
}

dependencies {
    api(project(":envoy-xds-core"))
    // In our public API's signatures
    api(libs.protobuf.java)
    api(libs.guava)
    api(libs.errorprone.annotations)
    compileOnly(libs.envoy.api) // Users are expected to provide a compatible envoy api
    testImplementation(libs.envoy.api)
    testImplementation(libs.hegel)
}

tasks.javadoc {
    (options as StandardJavadocDocletOptions).addBooleanOption("Werror", true) // every public API documented
}
