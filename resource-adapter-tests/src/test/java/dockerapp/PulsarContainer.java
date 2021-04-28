package dockerapp;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class PulsarContainer implements AutoCloseable {

    private GenericContainer<?> pulsarContainer;
    private final Network network;

    public PulsarContainer(Network network) {
        this.network = network;
    }


    public void start() throws Exception {
        CountDownLatch pulsarReady = new CountDownLatch(1);
        pulsarContainer = new GenericContainer<>("apachepulsar/pulsar:2.7.1")
                .withNetwork(network)
                .withNetworkAliases("pulsar")
                .withCommand("bin/pulsar", "standalone", "--advertised-address", "pulsar")
                .withLogConsumer(
                        (f) -> {
                            String text = f.getUtf8String().trim();
                            if (text.contains("messaging service is ready")) {
                                pulsarReady.countDown();
                            }
                            System.out.println(text);
                        });
            pulsarContainer.start();
            assertTrue(pulsarReady.await(1, TimeUnit.MINUTES));
    }

    @Override
    public void close() {
        if (pulsarContainer != null) {
            pulsarContainer.stop();
        }
    }
}
