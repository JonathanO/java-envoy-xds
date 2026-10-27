package net.woaf.jono.envoy.testing.configdump;

import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.net.HostAndPort;
import com.google.common.reflect.ClassPath;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import io.envoyproxy.envoy.admin.v3.ConfigDump;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Very simple client for the Envoy http admin interface.
 * Useful for writing end to end tests, where you might want to check envoy's config matches what you expected.
 */
public class EnvoyAdminClient {

    // Scanning the classpath takes seconds, so do it once.
    private static final Supplier<JsonFormat.Parser> PARSER = Suppliers.memoize(
        () -> JsonFormat.parser().ignoringUnknownFields().usingTypeRegistry(buildTypeRegistry()));

    private final HostAndPort hostAndPort;

    /**
     * A client for an Envoy's admin interface.
     * @param hostAndPort where it listens
     */
    public EnvoyAdminClient(HostAndPort hostAndPort) {
        this.hostAndPort = hostAndPort;
    }

    /**
     * Unpack an Any without checked exceptions.
     * @param thing Any to unpack
     * @param clazz Class to unpack as
     * @param <T> Type of the unpackaged message (should match clazz)
     * @return The unpacked message
     * @throws RuntimeException if things go wrong.
     */
    public static <T extends Message> T sneakyUnpack(Any thing, Class<T> clazz) {
        try {
            return thing.unpack(clazz);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static TypeRegistry buildTypeRegistry() {
        // Surely there is a better way than this?
        TypeRegistry.Builder typeRegistryBuilder = TypeRegistry.newBuilder();
        ClassPath cp;
        try {
            cp = ClassPath.from(ClassLoader.getSystemClassLoader());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        cp.getTopLevelClassesRecursive("io.envoyproxy.envoy").forEach(classinfo -> {
            Class<?> clazz;
            try {
                clazz = Class.forName(classinfo.getName());
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
            try {
                Method gd = clazz.getMethod("getDescriptor");
                if (gd.getReturnType().getName().equals("com.google.protobuf.Descriptors$Descriptor")) {
                    typeRegistryBuilder.add((Descriptors.Descriptor) gd.invoke(null));
                }
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ignored) {
                // Ignored
            }
        });
        return typeRegistryBuilder.build();
    }

    /**
     * Call the config_dump endpoint.
     * @return The config dump
     * @throws IOException if there was an IO error trying to call envoy's admin interface
     */
    public TypedEnvoyConfigDump configDump() throws IOException {
        ConfigDump.Builder dumpBuilder = ConfigDump.newBuilder();
        PARSER.get().merge(get("/config_dump"), dumpBuilder);
        return TypedEnvoyConfigDump.of(dumpBuilder.build());
    }

    /**
     * Call the stats endpoint.
     * @return counters and gauges by name (histograms and 64 bit hashes such as versions are left out)
     * @throws IOException if there was an IO error trying to call envoy's admin interface
     */
    public Map<String, Long> stats() throws IOException {
        Map<String, Long> stats = new HashMap<>();
        for (String line : Splitter.on('\n').split(get("/stats"))) {
            int sep = line.lastIndexOf(": ");
            if (sep > 0 && line.substring(sep + 2).matches("\\d{1,18}")) {
                stats.put(line.substring(0, sep), Long.parseLong(line.substring(sep + 2)));
            }
        }
        return stats;
    }

    /**
     * Call the clusters endpoint, for what Envoy has per host.
     * @return numeric values by name, everything before the last {@code ::}, e.g. {@code foo::127.0.0.2:9901::rq_total}
     * @throws IOException if there was an IO error trying to call envoy's admin interface
     */
    public Map<String, Long> clusters() throws IOException {
        Map<String, Long> values = new HashMap<>();
        for (String line : Splitter.on('\n').split(get("/clusters"))) {
            int sep = line.lastIndexOf("::");
            if (sep > 0 && line.substring(sep + 2).matches("\\d{1,18}")) {
                values.put(line.substring(0, sep), Long.parseLong(line.substring(sep + 2)));
            }
        }
        return values;
    }

    private String get(String path) throws IOException {
        URLConnection connection = new URL("http://" + hostAndPort.toString() + path).openConnection();
        try (InputStream response = connection.getInputStream()) {
            return new String(response.readAllBytes(), StandardCharsets.UTF_8);
        }
    }


}
