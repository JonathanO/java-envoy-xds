plugins {
    id("envoy-xds.java-conventions")
}

description = "Envoy xDS Example"
envoyXds.displayName = "Envoy xDS Example"

dependencies {
    api(libs.guava)
    api(libs.protobuf.java)
    api(libs.envoy.api)
    api(project(":envoy-xds-core"))
    implementation(libs.errorprone.annotations)
    implementation(libs.grpc.api)
    implementation(project(":envoy-xds-snapshot"))
    runtimeOnly(libs.logback.classic)
    runtimeOnly(libs.grpc.netty.shaded) // a gRPC server transport; envoy-api doesn't bring one
}
