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
package com.datastax.oss.pulsar.jms;

import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import org.junit.jupiter.api.extension.RegisterExtension;

public class JMSPublishFiltersBaseMemoryCacheTest extends JMSPublishFiltersBase {

  @RegisterExtension
  static PulsarContainerExtension pulsarContainer =
      new PulsarContainerExtension()
          .withEnv("PULSAR_PREFIX_transactionCoordinatorEnabled", "true")
          .withEnv("PULSAR_PREFIX_brokerInterceptorsDirectory", "/pulsar/interceptors")
          .withEnv("PULSAR_PREFIX_brokerInterceptors", "jms-publish-filters")
          .withEnv("PULSAR_PREFIX_jmsApplyFiltersOnPublish", "true")
          .withEnv("PULSAR_PREFIX_jmsFiltersOnPublishMaxMemoryMB", "110")
          .withEnv("PULSAR_PREFIX_jmsFiltersOnPublishThreads", "10")
          .withLogContainerOutput(true);

  @Override
  PulsarContainerExtension getPulsarContainer() {
    return pulsarContainer;
  }
}
