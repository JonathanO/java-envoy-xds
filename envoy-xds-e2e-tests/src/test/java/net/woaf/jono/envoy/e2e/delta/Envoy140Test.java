package net.woaf.jono.envoy.e2e.delta;

public class Envoy140Test extends EnvoyIntTest {

    @Override
    protected String getEnvoyImageName() {
        return "envoyproxy/envoy:v1.40-latest";
    }

    @Override
    protected boolean hasBrokenWildcardReconnect() {
        return false;
    }

}
