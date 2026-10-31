pluginManagement {
    includeBuild("build-logic")
    repositories {
        gradlePluginPortal()
    }
}

plugins {
    id("com.autonomousapps.build-health") version "3.20.0" // dependency analysis of every project: ./gradlew buildHealth
}

dependencyResolutionManagement {
    repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
    repositories {
        mavenCentral()
    }
}

rootProject.name = "java-envoy-xds"

// The conformance harness (net.woaf.jono.envoy:envoy-xds-testing), from the spec submodule: every port runs it.
includeBuild("spec/conformance")

include("envoy-xds-core")
include("envoy-xds-snapshot")
include("envoy-xds-example")
include("envoy-xds-e2e-tests")
include("envoy-xds-spec-tests")
