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

public class SessionInfo extends BaseCommand {

  public static final byte DATA_STRUCTURE_TYPE = CommandTypes.SESSION_INFO;

  protected SessionId sessionId;

  public SessionInfo() {
    sessionId = new SessionId();
  }

  public SessionInfo(SessionId sessionId) {
    this.sessionId = sessionId;
  }

  public byte getDataStructureType() {
    return DATA_STRUCTURE_TYPE;
  }

  /** @openwire:property version=1 cache=true */
  public SessionId getSessionId() {
    return sessionId;
  }

  public void setSessionId(SessionId sessionId) {
    this.sessionId = sessionId;
  }

  public RemoveInfo createRemoveCommand() {
    RemoveInfo command = new RemoveInfo(getSessionId());
    command.setResponseRequired(isResponseRequired());
    return command;
  }
}
