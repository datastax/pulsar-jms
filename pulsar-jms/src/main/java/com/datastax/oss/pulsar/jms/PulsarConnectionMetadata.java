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

import java.util.Collections;
import java.util.Enumeration;
import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;
import org.apache.pulsar.PulsarVersion;

class PulsarConnectionMetadata implements ConnectionMetaData {

  static final PulsarConnectionMetadata INSTANCE = new PulsarConnectionMetadata();
  /**
   * Gets the JMS API version.
   *
   * @return the JMS API version
   * @throws JMSException if the JMS provider fails to retrieve the metadata due to some internal
   *     error.
   */
  @Override
  public String getJMSVersion() throws JMSException {
    return "2.0";
  }

  /**
   * Gets the JMS major version number.
   *
   * @return the JMS API major version number
   * @throws JMSException if the JMS provider fails to retrieve the metadata due to some internal
   *     error.
   */
  @Override
  public int getJMSMajorVersion() throws JMSException {
    return 2;
  }

  /**
   * Gets the JMS minor version number.
   *
   * @return the JMS API minor version number
   * @throws JMSException if the JMS provider fails to retrieve the metadata due to some internal
   *     error.
   */
  @Override
  public int getJMSMinorVersion() throws JMSException {
    return 0;
  }

  /**
   * Gets the JMS provider name.
   *
   * @return the JMS provider name
   * @throws JMSException if the JMS provider fails to retrieve the metadata due to some internal
   *     error.
   */
  @Override
  public String getJMSProviderName() throws JMSException {
    return "Pulsar";
  }

  /**
   * Gets the JMS provider version.
   *
   * @return the JMS provider version
   * @throws JMSException if the JMS provider fails to retrieve the metadata due to some internal
   *     error.
   */
  @Override
  public String getProviderVersion() throws JMSException {
    return PulsarVersion.getVersion();
  }

  /**
   * Gets the JMS provider major version number.
   *
   * @return the JMS provider major version number
   * @throws JMSException if the JMS provider fails to retrieve the metadata due to some internal
   *     error.
   */
  @Override
  public int getProviderMajorVersion() throws JMSException {
    return 2;
  }

  /**
   * Gets the JMS provider minor version number.
   *
   * @return the JMS provider minor version number
   * @throws JMSException if the JMS provider fails to retrieve the metadata due to some internal
   *     error.
   */
  @Override
  public int getProviderMinorVersion() throws JMSException {
    return 0;
  }

  /**
   * Gets an enumeration of the JMSX property names.
   *
   * @return an Enumeration of JMSX property names
   * @throws JMSException if the JMS provider fails to retrieve the metadata due to some internal
   *     error.
   */
  @Override
  public Enumeration getJMSXPropertyNames() throws JMSException {
    return Collections.emptyEnumeration();
  }
}
