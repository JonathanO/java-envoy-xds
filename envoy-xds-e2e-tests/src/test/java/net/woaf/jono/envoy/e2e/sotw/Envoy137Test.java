package net.woaf.jono.envoy.e2e.sotw;

public class Envoy137Test extends EnvoyIntTest {

    @Override
    protected String getEnvoyImageName() {
        return "envoyproxy/envoy:v1.37-latest";
    }

}
