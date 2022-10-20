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
package com.datastax.oss.pulsar.jms.cli;

import com.datastax.oss.pulsar.jms.selectors.SelectorSupport;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.selector.ParseException;
import org.apache.pulsar.admin.cli.extensions.ParameterDescriptor;
import org.apache.pulsar.admin.cli.extensions.ParameterType;

@Slf4j
public abstract class SubscriptionBaseCommand extends TopicBaseCommand {

  private final boolean allowOverrideSubscriptionName;

  public SubscriptionBaseCommand(boolean allowOverrideSubscriptionName, String destinationType) {
    super(destinationType);
    this.allowOverrideSubscriptionName = allowOverrideSubscriptionName;
  }

  @Override
  public String description() {
    return "Create a Subscription with optionally a JMS Selector";
  }

  protected String getSubscription() {
    return getStringParameter("--subscription", "");
  }

  protected String getSelector() {
    return getStringParameter("--selector", "");
  }

  protected boolean isEnableFiltering() {
    return Boolean.parseBoolean(getStringParameter("--enable-filtering", "true"));
  }

  @Override
  protected void defineParameters(List<ParameterDescriptor> list) {
    super.defineParameters(list);
    if (allowOverrideSubscriptionName) {
      list.add(
              ParameterDescriptor.builder()
                      .description("Subscription")
                      .type(ParameterType.STRING)
                      .names(Arrays.asList("--subscription", "-sub"))
                      .required(true)
                      .build());
    }
    list.add(
        ParameterDescriptor.builder()
            .description("Enable filtering")
            .type(ParameterType.BOOLEAN)
            .names(Arrays.asList("--enable-filtering"))
            .build());
    list.add(
        ParameterDescriptor.builder()
            .description("Selector")
            .type(ParameterType.STRING)
            .names(Arrays.asList("--selector", "-s"))
            .required(false)
            .build());
  }
}
