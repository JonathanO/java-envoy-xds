import org.gradle.api.provider.Property

/** What a module tells the convention plugins about itself. */
interface EnvoyXdsExtension {
    /** Human-readable name, for the jar manifest and the published POM, e.g. "Envoy xDS Snapshot". */
    val displayName: Property<String>
}
