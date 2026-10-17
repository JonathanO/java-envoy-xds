package net.woaf.jono.envoy.e2e.utils;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class EnvoyContainer extends GenericContainer<EnvoyContainer> {

    public EnvoyContainer(String image) {
        super(DockerImageName.parse(image));
        // Old Envoy builds occasionally crash in tcmalloc at startup; wait for the admin API and retry if it never comes up.
        waitingFor(Wait.forHttp("/server_info").forPort(9901));
        withStartupAttempts(3);
    }
}
