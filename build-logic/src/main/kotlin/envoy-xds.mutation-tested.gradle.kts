// ./gradlew pitest mutation tests the module with its own tests. Every mutant must be killed; a method that can't be
// is annotated @DoNotMutate, saying why.

plugins {
    id("envoy-xds.java-conventions")
    id("info.solidsoft.pitest")
}

val libs = versionCatalogs.named("libs")

dependencies {
    pitest(libs.findLibrary("pitest-junit5").get())
}

pitest {
    pitestVersion = libs.findVersion("pitest").get().requiredVersion
    targetClasses = setOf("net.woaf.jono.envoy.xds*")
    outputFormats = setOf("XML", "HTML")
    threads = Runtime.getRuntime().availableProcessors()
    timestampedReports = false
    mutationThreshold = 100 // every mutant killed
}
