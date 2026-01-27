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
package com.datastax.oss.pulsar.jms.rar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import java.io.PrintWriter;
import java.io.StringWriter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class PulsarManagedConnectionFactoryTest {

  @Test
  public void testMergeConfiguration() throws Exception {
    PulsarManagedConnectionFactory spec = new PulsarManagedConnectionFactory();
    PulsarResourceAdapter adapter = new PulsarResourceAdapter();
    adapter.setConfiguration("bar");
    spec.setResourceAdapter(adapter);
    assertSame(adapter, spec.getResourceAdapter());
    String configuration = "foo";
    spec.setConfiguration(configuration);
    assertEquals("foo", spec.getMergedConfiguration());
    assertEquals("foo", spec.getConfiguration());

    spec.setConfiguration("");
    assertEquals("bar", spec.getMergedConfiguration());
    assertEquals("", spec.getConfiguration());

    spec.setConfiguration(null);
    assertEquals("bar", spec.getMergedConfiguration());
    assertEquals(null, spec.getConfiguration());

    spec.setConfiguration("{}");
    assertEquals("bar", spec.getMergedConfiguration());
    assertEquals("{}", spec.getConfiguration());

    spec.setConfiguration("{   }");
    assertEquals("bar", spec.getMergedConfiguration());
    assertEquals("{   }", spec.getConfiguration());

    PrintWriter useless = new PrintWriter(new StringWriter());
    spec.setLogWriter(useless);
    assertSame(useless, spec.getLogWriter());
  }

  @Test
  public void testSetLogWriter() throws Exception {
    PulsarManagedConnectionFactory spec = new PulsarManagedConnectionFactory();
    PrintWriter useless = new PrintWriter(new StringWriter());
    spec.setLogWriter(useless);
    assertSame(useless, spec.getLogWriter());
  }

  @Test
  public void testGetConnection() throws Exception {

    String configuration0 = "xxxx0";
    String configuration1 = "xxxx1";
    String configuration2 = "xxxx2";
    PulsarManagedConnectionFactory spec = new PulsarManagedConnectionFactory();

    PulsarConnectionFactory factory0 = mock(PulsarConnectionFactory.class);
    PulsarConnectionFactory factory1 = mock(PulsarConnectionFactory.class);
    PulsarConnectionFactory factory2 = mock(PulsarConnectionFactory.class);
    PulsarResourceAdapter adapter = mock(PulsarResourceAdapter.class);
    spec.setResourceAdapter(adapter);

    when(adapter.getConfiguration()).thenReturn(configuration0);
    when(adapter.getPulsarConnectionFactory(eq(configuration0))).thenReturn(factory0);
    when(adapter.getPulsarConnectionFactory(eq(configuration1))).thenReturn(factory1);
    when(adapter.getPulsarConnectionFactory(eq(configuration2))).thenReturn(factory2);
    spec.setConfiguration(configuration1);
    assertSame(factory1, spec.createConnectionFactory());

    spec.setConfiguration(configuration2);
    assertSame(factory2, spec.createConnectionFactory());

    // use AdapterConfiguration
    spec.setConfiguration("");
    assertSame(factory0, spec.createConnectionFactory());

    spec.setConfiguration(null);
    assertSame(factory0, spec.createConnectionFactory());
  }
}
