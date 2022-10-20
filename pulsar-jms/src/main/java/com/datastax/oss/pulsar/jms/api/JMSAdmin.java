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
package com.datastax.oss.pulsar.jms.api;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

/**
 * Admin API for JMS features.
 * This is meant to be like an extension of the PulsarAdmin Java API.
 */
public interface JMSAdmin {

    /**
     * Get the handle to a Queue.
     * A {@link Session#createQueue(String)} this method does not
     * fail in case the destination doesn't exist.
     * @param queue
     * @return the handle
     * @throws JMSException
     */
    Queue getQueue(String queue) throws JMSException;

    /**
     * Get the handle to a Topic.
     * A {@link Session#createQueue(String)} this method does not
     * fail in case the destination doesn't exist.
     * @param topic
     * @return the handle
     * @throws JMSException
     */
    Topic getTopic(String topic) throws JMSException;

    /**
     * Describe a Destination.
     * @param destination the destination
     * @return
     * @throws JMSException
     */
    JMSDestinationMetadata describe(Destination destination) throws JMSException;

    /**
     * Create a new Pulsar topic and set-up it as a Queue.
     * A JMS Queue maps to a Pulsar Subscription.
     * Create the topic if it does not exist.
     * Create the subscription if it does not exit.
     * If the topic already exists the API fails in case it doesn't match
     * the expected number of partitions.
     * If the subscription already exists an error is thrown.
     * @param queue the destination
     * @param partitions the number of partitions, 0 to create a non-partitioned topic
     * @param enableFilters enable the filters
     * @param selector selector
     * @throws JMSException
     */
    void createQueue(Queue queue, int partitions, boolean enableFilters, String selector) throws JMSException;

    /**
     * Create a new Pulsar topic and set-up it as a Queue.
     * A Topic maps to a Pulsar Topic.
     * Create the topic if it does not exist.
     * If the topic already exists the API fails in case it doesn't match
     * the expected number of partitions.
     * @param topic the destination
     * @param partitions the number of partitions, 0 to create a non-partitioned topic
     * @throws JMSException
     */
    void createTopic(Topic topic, int partitions) throws JMSException;

    /**
     * Set the filter on a Queue.
     * The name of the subscription is defined by the Destination ("jms-queue" is the default name).
     * @param destination the destination
     * @param enableFilters enable the filters
     * @param selector the selector
     * @throws JMSException
     */
    void setSubscriptionSelector(Queue destination, boolean enableFilters, String selector) throws JMSException;

    /**
     * Create a subscription on a JMS Topic.
     * @param destination the destination
     * @param subscriptionName the subscription name
     * @param enableFilters enable the filters
     * @param selector the selector
     * @param fromBeginning start the subscription from the beginning of the topic, otherwise it will start from the tail.
     * @throws JMSException
     * @see #createQueue(Queue, int, boolean, String)
     */
    void createSubscription(Topic destination, String subscriptionName, boolean enableFilters, String selector, boolean fromBeginning) throws JMSException;

    /**
     * Set the filter on a subscription on a Topic.
     * @param destination the destination
     * @param subscriptionName the subscription name
     * @param enableFilters enable the filters
     * @param selector the selector
     * @throws JMSException
     */
    void setSubscriptionSelector(Topic destination, String subscriptionName, boolean enableFilters, String selector) throws JMSException;

}
