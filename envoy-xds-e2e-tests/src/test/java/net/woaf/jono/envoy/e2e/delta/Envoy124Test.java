package net.woaf.jono.envoy.e2e.delta;

public class Envoy124Test extends EnvoyIntTest {

    @Override
    protected String getEnvoyImageName() {
        return "envoyproxy/envoy:v1.24-latest";
    }

}
