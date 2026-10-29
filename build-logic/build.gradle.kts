plugins {
    `kotlin-dsl`
}

// The plugins the convention plugins apply.
dependencies {
    implementation(libs.errorprone.plugin)
    implementation(libs.pitest.plugin)
    implementation(libs.spotbugs.plugin)
}
