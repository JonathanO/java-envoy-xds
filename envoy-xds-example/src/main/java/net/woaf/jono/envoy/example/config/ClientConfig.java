package net.woaf.jono.envoy.example.config;

/**
 * What the example's config builders need to know about a client.
 *
 * @param listenPort Port on which the envoy instance is listening for http requests to forward.
 */
public record ClientConfig(long listenPort) {

    public static final long DEFAULT_LISTEN_PORT = 8080;
}
