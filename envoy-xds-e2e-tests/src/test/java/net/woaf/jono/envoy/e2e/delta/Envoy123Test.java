package net.woaf.jono.envoy.e2e.delta;

public class Envoy123Test extends EnvoyIntTest {

    @Override
    protected String getEnvoyImageName() {
        return "envoyproxy/envoy:v1.23-latest";
    }

}
