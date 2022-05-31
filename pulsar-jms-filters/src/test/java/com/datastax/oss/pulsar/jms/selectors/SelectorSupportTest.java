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
package com.datastax.oss.pulsar.jms.selectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SelectorSupportTest {

  @Test
  public void test() throws Exception {
    match(true, "foo='bar' or foo='bar' or foo='bar'");
    match(true, "foo='bar' or foo='bar' or foo='other'");
    match(false, "foo='baz' or foo='other'");
    match(true, "foo='baz' or foo='bar'");

    match(true, "foo='bar' and foo='bar' and foo='bar'");
    match(false, "foo='bar' and foo='bar' and foo='other'");
    match(false, "foo='bar' and foo='other'");
    match(true, "foo='bar' and foo='bar'");
    match(false, "foo='other'");
    match(false, "foo is null");
    match(true, "foo is not null");
    match(true, "undefinedProperty is null");
    match(false, "not undefinedProperty");
    match(false, "undefinedProperty");
  }

  private static void match(boolean expected, String selector) throws Exception {
    SelectorSupport build = SelectorSupport.build(selector, true);
    Map<String, Object> properties = new HashMap<>();
    properties.put("foo", "bar");
    assertEquals(expected, build.matches(properties, "", "", null, null, 0, null, 0, 0, 0));
  }
}
