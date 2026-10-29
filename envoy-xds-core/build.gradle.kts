plugins {
    id("envoy-xds.published")
    id("envoy-xds.mutation-tested")
}

description = "Envoy xDS"
envoyXds.displayName = "Envoy xDS"

dependencies {
    // In our public API's signatures
    api(libs.protobuf.java)
    api(libs.guava)
    api(libs.grpc.stub)
    api(libs.errorprone.annotations)
    implementation(libs.grpc.api)
    implementation(libs.slf4j.api)
    compileOnly(libs.envoy.api) // Users are expected to provide a compatible envoy api
    testImplementation(libs.envoy.api)
    testImplementation(libs.spotbugs.annotations)
    testImplementation(libs.hegel)
    testImplementation(libs.logback.classic) // to check what we warn about
    testImplementation(libs.logback.core)
}

tasks.javadoc {
    exclude("**/internal/**") // not API
    (options as StandardJavadocDocletOptions).addBooleanOption("Werror", true) // every public API documented
}
