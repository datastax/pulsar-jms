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
package io.streamnative.oss.pulsar.jms.tests;

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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.MountableFile;

public class DockerTest {

  @TempDir public Path temporaryDir;

  public Path createDeployment() throws Exception {
    Path path = temporaryDir.resolve("test.war");
    // create a test WebApplication that contains our JMS/JavaEE code
    Archive<?> war =
        ShrinkWrap.create(WebArchive.class, path.getFileName().toString())
            .addPackage(SendJMSMessage.class.getPackage())
            .addAsWebInfResource(EmptyAsset.INSTANCE, "beans.xml");
    war.as(ZipExporter.class).exportTo(path.toFile());
    return path;
  }

  @Test
  public void test() throws Exception {
    int numMessages = 20; // more then 10 please
    CountDownLatch allMessagesReceived = new CountDownLatch(1);

    Path applicationFile = createDeployment();

    // this file is downloaded by maven-dependency-plugin
    Path pulsarraFile = Paths.get("target/pulsarra.rar");

    // tomee.xml file
    Path tomeeFile = Paths.get("src/main/resources/tomee.xml");

    // server wide configuration
    Path systemPropertiesFile = Paths.get("src/main/resources/system.properties");

    // create a docker network
    try (Network network = Network.newNetwork(); ) {
      // start Pulsar and wait for it to be ready to accept requests
      try (PulsarContainer pulsarContainer = new PulsarContainer(network); ) {
        pulsarContainer.start();

        // start TomEE
        // deploy these files to the container:
        // - tomee.xml
        // - conf/system.properties
        // - ResourceAdapter pulsarra.rar
        // - WebApp
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
                      String text = f.getUtf8String().trim();
                      if (text.contains("TOTAL MESSAGES -" + numMessages + "-")) {
                        allMessagesReceived.countDown();
                      }
                      System.out.println(text);
                    })) {
          container.start();

          // expect the application to write an expected log
          assertTrue(allMessagesReceived.await(2, TimeUnit.MINUTES));
        }
      }
    }
  }
}
