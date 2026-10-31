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

// Replay mode, for the spec's differential fuzzer (docs/REPLAY.md there): writes build/replay, a script that runs it.
val replayScript by tasks.registering {
    description = "Writes build/replay, which replays trace vectors from stdin, for the spec's differential fuzzer."
    val classpath = sourceSets.test.get().runtimeClasspath
    val script = layout.buildDirectory.file("replay")
    dependsOn(tasks.testClasses)
    inputs.files(classpath)
    outputs.file(script)
    doLast {
        val java = File(System.getProperty("java.home"), "bin/java")
        script.get().asFile.writeText("#!/bin/sh\nexec '$java' -cp '${classpath.asPath}' net.woaf.jono.envoy.xds.spec.Replay\n")
        script.get().asFile.setExecutable(true)
    }
}
