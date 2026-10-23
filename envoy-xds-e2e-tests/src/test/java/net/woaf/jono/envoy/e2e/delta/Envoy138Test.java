package net.woaf.jono.envoy.e2e.delta;

public class Envoy138Test extends EnvoyIntTest {

    @Override
    protected String getEnvoyImageName() {
        return "envoyproxy/envoy:v1.38-latest";
    }

}
