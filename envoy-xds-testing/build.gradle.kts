plugins {
    id("envoy-xds.published")
}

description = "Test a control plane against a real Envoy: record the xDS conversation and check it, Envoy's stats and its log"
envoyXds.displayName = "Envoy xDS Testing"

// Deliberately independent of envoy-xds-core, so any Java control plane can use it.
dependencies {
    api(libs.guava)
    api(libs.protobuf.java)
    api(libs.grpc.api)
    api(libs.assertj)
    api(libs.testcontainers)
    api(libs.junit.jupiter.api) // for the conformance suite
    api(libs.hamcrest) // for XdsMatchers
    implementation(libs.awaitility)
    implementation(libs.errorprone.annotations)
    implementation(libs.protobuf.java.util)
    implementation(libs.slf4j.api)
    compileOnly(libs.spotbugs.annotations) // EnvoyContainer's @SuppressFBWarnings
    compileOnly(libs.envoy.api) // Users are expected to provide a compatible envoy api
    testImplementation(libs.envoy.api)
}

tasks.javadoc {
    (options as StandardJavadocDocletOptions).addBooleanOption("Werror", true) // every public API documented
}
