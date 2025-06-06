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
package org.apache.activemq.command;

import java.io.IOException;
import org.apache.activemq.wireformat.WireFormat;

public interface MarshallAware {

  void beforeMarshall(WireFormat wireFormat) throws IOException;

  void afterMarshall(WireFormat wireFormat) throws IOException;

  void beforeUnmarshall(WireFormat wireFormat) throws IOException;

  void afterUnmarshall(WireFormat wireFormat) throws IOException;
}
