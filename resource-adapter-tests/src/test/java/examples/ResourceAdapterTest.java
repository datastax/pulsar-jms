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
package examples;

import java.io.File;
import javax.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.EnterpriseArchive;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.ResourceAdapterArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ArquillianExtension.class)
public class ResourceAdapterTest {

  @Deployment
  public static EnterpriseArchive deploy() throws Exception {

    ResourceAdapterArchive rar =
        ShrinkWrap.createFromZipFile(ResourceAdapterArchive.class, new File("target/pulsarra.rar"));

    JavaArchive ejbjar =
        ShrinkWrap.create(JavaArchive.class, "ejb.jar")
            .addClasses(SendJMSMessage.class)
            .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");

    JavaArchive libjar =
        ShrinkWrap.create(JavaArchive.class, "lib.jar")
            .addClasses(ResourceAdapterTest.class)
            .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");

    EnterpriseArchive ear =
        ShrinkWrap.create(EnterpriseArchive.class, "test.ear")
            .addAsModules(rar, ejbjar)
            .addAsResource(new File("src/main/resources/META-INF/resources.xml"), "resources.xml")
            .addAsLibraries(libjar);
    return ear;
  }

  @Inject SendJMSMessage service;

  @Test
  public void test() throws Exception {
    System.out.println("Service: " + service);
    Thread.sleep(Integer.MAX_VALUE);
    service.doSend();
  }
}
