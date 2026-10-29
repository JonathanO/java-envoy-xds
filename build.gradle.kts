plugins {
    idea
    alias(libs.plugins.nexus.publish)
}

dependencyAnalysis {
    // JUnit's aggregate artifact is how JUnit says to depend on it.
    structure {
        bundle("junit-jupiter") {
            includeGroup("org.junit.jupiter")
        }
    }
    issues {
        all {
            onUnusedDependencies {
                // envoy-xds.java-conventions gives every module the same test kit.
                exclude("org.assertj:assertj-core", "org.mockito:mockito-core", "org.mockito:mockito-junit-jupiter",
                    "org.junit.jupiter:junit-jupiter")
            }
        }
    }
}

idea {
    project {
        vcs = "Git"
        languageLevel = org.gradle.plugins.ide.idea.model.IdeaLanguageLevel(JavaVersion.VERSION_17)
        targetBytecodeVersion = JavaVersion.VERSION_17
    }
    module {
        isDownloadJavadoc = false
        isDownloadSources = true
        excludeDirs.addAll(listOf("logs", "gradle", "build", "target", ".idea", ".gradle", "logs2").map(::file))
    }
}

group = "net.woaf.jono.envoy"

nexusPublishing {
    repositories {
        sonatype {
            // Maven Central Portal's OSSRH-compatible staging API: OSSRH itself was retired in 2025.
            nexusUrl = uri("https://ossrh-staging-api.central.sonatype.com/service/local/")
            snapshotRepositoryUrl = uri("https://central.sonatype.com/repository/maven-snapshots/")
            username = providers.environmentVariable("SONATYPE_USER")
            password = providers.environmentVariable("SONATYPE_PASSWORD")
        }
    }
}
