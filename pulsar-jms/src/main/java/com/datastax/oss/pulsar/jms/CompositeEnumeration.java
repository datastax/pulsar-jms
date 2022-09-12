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

import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;

public final class CompositeEnumeration implements Enumeration {
  private final List<? extends Enumeration> enumerations;
  private int currentEnumeration = 0;
  private Enumeration current;

  public CompositeEnumeration(List<? extends Enumeration> enumerations) {
    this.enumerations = enumerations;
    if (enumerations.isEmpty()) {
      current = null;
      currentEnumeration = -1;
    } else {
      startEnumeration(0);
    }
  }

  private void startEnumeration(int n) {
    if (n == enumerations.size()) {
      currentEnumeration = -1;
      current = null;
    } else {
      currentEnumeration = n;
      this.current = enumerations.get(n);
      skipEmpty();
    }
  }

  private void skipEmpty() {
    while (!current.hasMoreElements()) {
      currentEnumeration++;
      if (currentEnumeration == enumerations.size()) {
        currentEnumeration = -1;
        current = null;
        break;
      }
      this.current = enumerations.get(currentEnumeration);
    }
  }

  @Override
  public synchronized boolean hasMoreElements() {
    return currentEnumeration >= 0;
  }

  @Override
  public synchronized Object nextElement() {
    if (current == null) {
      throw new NoSuchElementException();
    }
    Object next = current.nextElement();
    if (!current.hasMoreElements()) {
      startEnumeration(currentEnumeration + 1);
    }
    return next;
  }
}
