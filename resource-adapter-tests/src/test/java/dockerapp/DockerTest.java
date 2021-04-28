/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dockerapp;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.MountableFile;

@Disabled // I cannot make it work the Configuration of the ConnectionFactory
public class DockerTest {

  @TempDir public Path temporaryDir;

  public static Archive<?> createDeployment() {
    return ShrinkWrap.create(WebArchive.class, "test.war")
        .addPackage(SendJMSMessage.class.getPackage())
        .addAsWebInfResource(EmptyAsset.INSTANCE, "beans.xml");
  }

  @Test
  public void test() throws Exception {

    Path applicationFile = temporaryDir.resolve("test.war");
    Archive<?> ear = createDeployment();
    ear.as(ZipExporter.class).exportTo(applicationFile.toFile());

    Path pulsarraFile = Paths.get("target/pulsarra.rar");
    Path tomeeFile = Paths.get("src/main/resources/tomee.xml");
    Path systemPropertiesFile = Paths.get("src/main/resources/system.properties");
    CountDownLatch pulsarReady = new CountDownLatch(1);
    try (Network network = Network.newNetwork(); ) {
      try (GenericContainer<?> pulsarContainer =
          new GenericContainer<>("apachepulsar/pulsar:2.7.1")
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
                  })) {
        pulsarContainer.start();
        assertTrue(pulsarReady.await(1, TimeUnit.MINUTES));

        try (GenericContainer<?> container =
            new GenericContainer<>("tomee:11-jre-8.0.6-plus")
                .withNetwork(network)
                .withCopyFileToContainer(
                    MountableFile.forHostPath(pulsarraFile), "/usr/local/tomee/rars/pulsarra.rar")
                .withCopyFileToContainer(
                    MountableFile.forHostPath(systemPropertiesFile),
                    "/usr/local/tomee/conf/system.properties")
                .withCopyFileToContainer(
                    MountableFile.forHostPath(tomeeFile), "/usr/local/tomee/conf/tomee.xml")
                .withCopyFileToContainer(
                    MountableFile.forHostPath(applicationFile), "/usr/local/tomee/webapps/test.war")
                .withLogConsumer(
                    (f) -> {
                      System.out.println(f.getUtf8String());
                    })) {

          Thread.sleep(10000);
          container.start();
          Thread.sleep(Integer.MAX_VALUE);
        }
      }
    }
  }
}
