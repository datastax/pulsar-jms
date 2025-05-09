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
package org.apache.activemq.jndi;

import java.util.Properties;
import javax.naming.Referenceable;

/** Facilitates objects to be stored in JNDI as properties */
public interface JNDIStorableInterface extends Referenceable {

  /**
   * set the properties for this instance as retrieved from JNDI
   *
   * @param properties properties
   */
  void setProperties(Properties properties);

  /**
   * Get the properties from this instance for storing in JNDI
   *
   * @return the properties that should be stored in JNDI
   */
  Properties getProperties();
}
