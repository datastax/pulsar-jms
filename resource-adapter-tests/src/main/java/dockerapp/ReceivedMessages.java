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
package dockerapp;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

@ApplicationScoped
public class ReceivedMessages {
  private final List<String> messages = new CopyOnWriteArrayList<>();

  public List<String> getMessages() {
    return messages;
  }

  public void onMessage(@Observes final ReceivedMessage onJmsMessage) {
    System.out.println("onMessage " + onJmsMessage);
    messages.add(onJmsMessage.getMessage());
    // this output is checked by the test case, do not change
    System.out.println("TOTAL MESSAGES -" + messages.size()+"-");
  }
}
