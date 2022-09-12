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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

public class CompositeEnumerationTest {

  static List<String> LIST1 = Arrays.asList("a", "b", "c");
  static List<String> LIST2 = Arrays.asList("d", "e", "f");
  static List<String> LIST3 = Arrays.asList("g", "h", "i");

  @Test
  public void testEmpty() {
    CompositeEnumeration empty = new CompositeEnumeration(Collections.emptyList());
    assertFalse(empty.hasMoreElements());
    assertThrows(NoSuchElementException.class, () -> empty.nextElement());
  }

  @Test
  public void testWrapEmpty() {
    List<Enumeration> list = new ArrayList<>();
    list.add(Collections.emptyEnumeration());
    CompositeEnumeration empty = new CompositeEnumeration(list);
    assertFalse(empty.hasMoreElements());
    assertThrows(NoSuchElementException.class, () -> empty.nextElement());
  }

  @Test
  public void testWrap2Empty() {
    List<Enumeration> list = new ArrayList<>();
    list.add(Collections.emptyEnumeration());
    list.add(Collections.emptyEnumeration());
    CompositeEnumeration empty = new CompositeEnumeration(list);
    assertFalse(empty.hasMoreElements());
    assertThrows(NoSuchElementException.class, () -> empty.nextElement());
  }

  @Test
  public void testWrap3Empty() {
    List<Enumeration> list = new ArrayList<>();
    list.add(Collections.emptyEnumeration());
    list.add(Collections.emptyEnumeration());
    list.add(Collections.emptyEnumeration());
    CompositeEnumeration empty = new CompositeEnumeration(list);
    assertFalse(empty.hasMoreElements());
    assertThrows(NoSuchElementException.class, () -> empty.nextElement());
  }

  @Test
  public void testWrapSingle() {
    List<Enumeration> list = new ArrayList<>();
    list.add(Collections.enumeration(LIST1));
    CompositeEnumeration e = new CompositeEnumeration(list);
    assertEquals(LIST1, scan(e));
  }

  @Test
  public void testWrap2() {
    List<Enumeration> list = new ArrayList<>();
    list.add(Collections.enumeration(LIST1));
    list.add(Collections.enumeration(LIST2));
    CompositeEnumeration e = new CompositeEnumeration(list);
    List<String> concat = new ArrayList<>();
    concat.addAll(LIST1);
    concat.addAll(LIST2);
    assertEquals(concat, scan(e));
  }

  @Test
  public void testWrap3() {
    List<Enumeration> list = new ArrayList<>();
    list.add(Collections.enumeration(LIST1));
    list.add(Collections.enumeration(LIST2));
    list.add(Collections.enumeration(LIST3));
    CompositeEnumeration e = new CompositeEnumeration(list);
    List<String> concat = new ArrayList<>();
    concat.addAll(LIST1);
    concat.addAll(LIST2);
    concat.addAll(LIST3);
    assertEquals(concat, scan(e));
  }

  @Test
  public void testWrapEmptyMiddle() {
    List<Enumeration> list = new ArrayList<>();
    list.add(Collections.enumeration(LIST1));
    list.add(Collections.enumeration(Collections.EMPTY_LIST));
    list.add(Collections.enumeration(LIST3));
    CompositeEnumeration e = new CompositeEnumeration(list);
    List<String> concat = new ArrayList<>();
    concat.addAll(LIST1);
    concat.addAll(LIST3);
    assertEquals(concat, scan(e));
  }

  private static List<String> scan(Enumeration e) {
    List<String> result = new ArrayList<>();
    while (e.hasMoreElements()) {
      result.add((String) e.nextElement());
    }
    assertThrows(NoSuchElementException.class, () -> e.nextElement());
    return result;
  }
}
