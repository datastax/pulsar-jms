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
package org.apache.activemq.filter.function;

import org.apache.activemq.filter.FunctionCallExpression;

/**
 * Registry of built-in functions. Add built-in functions to this list to make sure they are
 * registered at startup.
 *
 * <p>Custom add-ons that are not built-in to the core ActiveMQ should not be listed here. Use
 * FunctionCallExpression.registerFunction() directly.
 */
public class BuiltinFunctionRegistry {
  public static void register() {
    FunctionCallExpression.registerFunction("INLIST", new inListFunction());
    FunctionCallExpression.registerFunction("MAKELIST", new makeListFunction());
    FunctionCallExpression.registerFunction("REGEX", new regexMatchFunction());
    FunctionCallExpression.registerFunction("REPLACE", new replaceFunction());
    FunctionCallExpression.registerFunction("SPLIT", new splitFunction());
  }
}
