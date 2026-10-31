plugins {
    id("envoy-xds.java-conventions")
}

description = "Envoy xDS Example E2E Tests"
envoyXds.displayName = "Envoy xDS Example E2E Tests"

dependencies {
    api(project(":envoy-xds-core"))
    api("net.woaf.jono.envoy:envoy-xds-testing") // the spec's conformance/, an included build
    testImplementation(libs.guava)
    testImplementation(libs.protobuf.java)
    testImplementation(libs.envoy.api)
    testImplementation(project(":envoy-xds-snapshot"))
    testImplementation(libs.grpc.api)
    testRuntimeOnly(libs.grpc.netty.shaded)
    testRuntimeOnly(libs.logback.classic) // so our and Testcontainers' logs reach the test output
    testImplementation(libs.testcontainers)
    testImplementation(libs.awaitility)
}

tasks.test {
    // CI runs EnvoyXdsConformanceTest on one Envoy version per job; without this it runs on all of them.
    providers.gradleProperty("envoyVersions").orNull?.let { systemProperty("envoyVersions", it) }
    // How many tests run at once, each with its own Envoy; the default is in junit-platform.properties.
    providers.gradleProperty("testParallelism").orNull?.let {
        systemProperty("junit.jupiter.execution.parallel.config.fixed.parallelism", it)
    }
}
