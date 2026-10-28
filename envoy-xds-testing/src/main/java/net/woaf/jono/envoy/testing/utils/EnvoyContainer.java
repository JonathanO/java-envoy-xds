package net.woaf.jono.envoy.testing.utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.net.HostAndPort;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import net.woaf.jono.envoy.testing.configdump.EnvoyAdminClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/**
 * An Envoy container that keeps its log. Envoy's config must put the admin interface on 0.0.0.0:9901.
 */
@SuppressFBWarnings(value = "EQ_DOESNT_OVERRIDE_EQUALS", justification = "The captured log isn't part of the container's identity")
public class EnvoyContainer extends GenericContainer<EnvoyContainer> {

    /** The admin interface's port inside the container. */
    public static final int ADMIN_PORT = 9901;

    /**
     * What {@link #withBootstrap} replaces with the control plane's port, e.g. {@code port_value: ${ADS_PORT}}.
     */
    public static final String ADS_PORT_PLACEHOLDER = "${ADS_PORT}";

    private final List<String> log = Collections.synchronizedList(new ArrayList<>());

    /**
     * An Envoy container, retried if Envoy doesn't start.
     * @param image e.g. envoyproxy/envoy:v1.40.0
     */
    @SuppressWarnings("this-escape") // the usual Testcontainers set-up; subclasses get our defaults first
    public EnvoyContainer(String image) {
        super(DockerImageName.parse(image));
        // Old Envoy builds occasionally crash in tcmalloc at startup; wait for the admin API and retry if it never comes up.
        waitingFor(Wait.forHttp("/server_info").forPort(ADMIN_PORT));
        withStartupAttempts(3);
        withLogConsumer(frame -> log.add(frame.getUtf8StringWithoutLineEnding()));
    }

    /**
     * Runs Envoy with a bootstrap config from the classpath, with {@link #ADS_PORT_PLACEHOLDER} replaced by your
     * control plane's port. Bind your server to port 0, then pass its port here and to
     * {@code Testcontainers.exposeHostPorts}, so tests can run in parallel.
     * @param resource the config's classpath resource, e.g. envoy.yaml
     * @param adsPort your control plane's port on the host
     * @return this
     */
    public EnvoyContainer withBootstrap(String resource, int adsPort) {
        String yaml;
        try (InputStream in = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(resource), resource)) {
            yaml = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Preconditions.checkArgument(yaml.contains(ADS_PORT_PLACEHOLDER), "%s has no %s", resource, ADS_PORT_PLACEHOLDER);
        return withCopyToContainer(Transferable.of(yaml.replace(ADS_PORT_PLACEHOLDER, Integer.toString(adsPort))), "/envoy.yaml")
            .withCommand("-c", "/envoy.yaml");
    }

    /**
     * Returns a client for the running container's admin interface.
     * @return admin client
     */
    public EnvoyAdminClient admin() {
        return new EnvoyAdminClient(HostAndPort.fromParts(getHost(), getMappedPort(ADMIN_PORT)));
    }

    /**
     * Log lines so far, from fromLine on.
     * @param fromLine index of the first line, e.g. a previous {@link #logSize()}
     * @return a copy of those lines
     */
    public List<String> logSince(int fromLine) {
        synchronized (log) { // Docker is still appending
            return new ArrayList<>(log.subList(fromLine, log.size()));
        }
    }

    /**
     * {@return how many log lines so far}
     */
    public int logSize() {
        return log.size();
    }

    /**
     * Status code of a GET through one of Envoy's HTTP listeners.
     * @param listenerPort the listener's port in the container, which must be exposed and listening on 0.0.0.0
     * @param virtualHost Host header to send
     * @return HTTP status code
     * @throws IOException if the request fails
     */
    public int httpStatus(int listenerPort, String virtualHost) throws IOException {
        try (Socket socket = new Socket(getHost(), getMappedPort(listenerPort))) {
            socket.setSoTimeout(5000);
            socket.getOutputStream().write(("GET / HTTP/1.1\r\nHost: " + virtualHost + "\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
            String statusLine = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII)).readLine();
            return Integer.parseInt(Splitter.on(' ').splitToList(statusLine).get(1));
        }
    }
}
