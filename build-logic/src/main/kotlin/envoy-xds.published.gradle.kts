// A module published to Maven Central, as net.woaf.jono.envoy:<module name>. Signed when CI has the key.

plugins {
    id("envoy-xds.java-conventions")
    `maven-publish`
    signing
}

val envoyXds = the<EnvoyXdsExtension>()

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            pom {
                name = envoyXds.displayName
                description = provider { project.description }
                url = "https://github.com/JonathanO/java-envoy-xds"
                packaging = "jar"
                licenses {
                    license {
                        name = "The Apache License, Version 2.0, Copyright 2022 Wise Plc, 2026 Jonathan Oddy"
                        url = "http://www.apache.org/licenses/LICENSE-2.0.txt"
                    }
                }
                developers {
                    developer {
                        id = "JonathanO"
                        name = "Jonathan Oddy"
                        email = "jonathan@woaf.net"
                    }
                }
                scm {
                    connection = "scm:git:git://github.com/JonathanO/java-envoy-xds.git"
                    developerConnection = "scm:git:git://github.com/JonathanO/java-envoy-xds.git"
                    url = "https://github.com/JonathanO/java-envoy-xds"
                }
            }
        }
    }
}

val signingKey = providers.environmentVariable("OSS_SIGNING_KEY")
if (signingKey.isPresent) {
    signing {
        useInMemoryPgpKeys(signingKey.get(), providers.environmentVariable("OSS_SIGNING_PASSWORD").orNull)
        sign(publishing.publications["mavenJava"])
    }
}
