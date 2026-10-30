plugins {
    id("envoy-xds.java-conventions")
}

description = "Runs the envoy-xds-spec trace vectors against this library"
envoyXds.displayName = "Envoy xDS Spec Tests"

// Tests only: the runner is test code, and there is no production code to mutation test.
dependencies {
    testImplementation(project(":envoy-xds-core"))
    testImplementation(project(":envoy-xds-snapshot"))
    testImplementation(libs.envoy.api)
    testImplementation(libs.protobuf.java)
    testImplementation(libs.protobuf.java.util)
    testImplementation(libs.gson)
    testImplementation(libs.guava)
    testImplementation(libs.grpc.stub)
}

// The vectors come from the spec submodule: git submodule update --init
val vectors = rootProject.layout.projectDirectory.dir("spec/vectors")

tasks.test {
    inputs.dir(vectors).withPropertyName("vectors")
    systemProperty("vectors", vectors.asFile.absolutePath)
}
