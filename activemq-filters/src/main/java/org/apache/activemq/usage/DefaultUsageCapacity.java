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
package org.apache.activemq.usage;

/**
 * Identify if a limit has been reached
 *
 */
public class DefaultUsageCapacity implements UsageCapacity {

  private long limit;

  /**
   * @param size
   * @return true if the limit is reached
   * @see UsageCapacity#isLimit(long)
   */
  public boolean isLimit(long size) {
    return size >= limit;
  }

  /** @return the limit */
  public final long getLimit() {
    return this.limit;
  }

  /** @param limit the limit to set */
  public final void setLimit(long limit) {
    this.limit = limit;
  }
}
