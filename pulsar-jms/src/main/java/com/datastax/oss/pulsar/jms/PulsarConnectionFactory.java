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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import javax.jms.ConnectionFactory;
import javax.jms.IllegalStateException;
import javax.jms.InvalidClientIDException;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.JMSSecurityException;
import javax.jms.JMSSecurityRuntimeException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;

@Slf4j
public class PulsarConnectionFactory
    implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory, AutoCloseable {
  private static final Set<String> clientIdentifiers = new ConcurrentSkipListSet<>();

  private final PulsarClient pulsarClient;
  private final PulsarAdmin pulsarAdmin;
  private final Map<String, Object> producerConfiguration;
  private final Map<String, Object> consumerConfiguration;
  private final Map<String, Producer<byte[]>> producers = new ConcurrentHashMap<>();
  private final Set<PulsarConnection> connections = Collections.synchronizedSet(new HashSet<>());
  private final List<Consumer<byte[]>> consumers = new CopyOnWriteArrayList<>();
  private final List<Reader<byte[]>> readers = new CopyOnWriteArrayList<>();
  private final String systemNamespace;
  private final String defaultClientId;
  private final boolean enableTransaction;
  private final boolean enableClientSideFeatures;
  private final boolean forceDeleteTemporaryDestinations;
  private final boolean useExclusiveSubscriptionsForSimpleConsumers;
  private final String tckUsername;
  private final String tckPassword;
  private final String queueSubscriptioName;

  @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NULL_VALUE")
  public PulsarConnectionFactory(Map<String, Object> properties) throws JMSException {
    try {
      properties = new HashMap(properties);

      Map<String, Object> producerConfiguration =
          (Map<String, Object>) properties.remove("producerConfig");
      if (producerConfiguration != null) {
        this.producerConfiguration = new HashMap(producerConfiguration);
      } else {
        this.producerConfiguration = Collections.emptyMap();
      }

      Map<String, Object> consumerConfigurationM =
          (Map<String, Object>) properties.remove("consumerConfig");
      if (consumerConfigurationM != null) {
        this.consumerConfiguration = new HashMap(consumerConfigurationM);
      } else {
        this.consumerConfiguration = Collections.emptyMap();
      }
      this.systemNamespace =
          getAndRemoveString("jms.systemNamespace", "public/default", properties);

      this.tckUsername = getAndRemoveString("jms.tckUsername", "", properties);
      this.tckPassword = getAndRemoveString("jms.tckPassword", "", properties);

      this.defaultClientId = getAndRemoveString("jms.clientId", null, properties);

      this.queueSubscriptioName = getAndRemoveString("jms.queueName", "jms-queue", properties);

      this.enableClientSideFeatures =
          Boolean.parseBoolean(
              getAndRemoveString("jms.enableClientSideFeatures", "false", properties));

      // in Exclusive mode Pulsar does not support delayed messages
      // with this flag you force to not use Exclusive subscription and so to support
      // delayed messages are well
      this.useExclusiveSubscriptionsForSimpleConsumers =
          Boolean.parseBoolean(
              getAndRemoveString(
                  "jms.useExclusiveSubscriptionsForSimpleConsumers", "true", properties));

      // default is false
      this.forceDeleteTemporaryDestinations =
          Boolean.parseBoolean(
              getAndRemoveString("jms.forceDeleteTemporaryDestinations", "false", properties));

      this.enableTransaction =
          Boolean.parseBoolean(properties.getOrDefault("enableTransaction", "false").toString());

      String webServiceUrl =
          getAndRemoveString("webServiceUrl", "http://localhost:8080", properties);

      String brokenServiceUrl = getAndRemoveString("brokerServiceUrl", "", properties);

      PulsarClient pulsarClient = null;
      PulsarAdmin pulsarAdmin = null;
      try {

        // must be the same as
        // https://pulsar.apache.org/docs/en/security-tls-keystore/#configuring-clients
        String authPluginClassName = getAndRemoveString("authPlugin", "", properties);
        String authParamsString = getAndRemoveString("authParams", "", properties);
        Authentication authentication =
            AuthenticationFactory.create(authPluginClassName, authParamsString);
        if (log.isDebugEnabled()) {
          log.debug("Authentication {}", authentication);
        }
        boolean tlsAllowInsecureConnection =
            Boolean.parseBoolean(
                getAndRemoveString("tlsAllowInsecureConnection", "false", properties));

        boolean tlsEnableHostnameVerification =
            Boolean.parseBoolean(
                getAndRemoveString("tlsEnableHostnameVerification", "false", properties));
        final String tlsTrustCertsFilePath =
            (String) getAndRemoveString("tlsTrustCertsFilePath", "", properties);

        boolean useKeyStoreTls =
            Boolean.parseBoolean(getAndRemoveString("useKeyStoreTls", "false", properties));
        String tlsTrustStoreType = getAndRemoveString("tlsTrustStoreType", "JKS", properties);
        String tlsTrustStorePath = getAndRemoveString("tlsTrustStorePath", "", properties);
        String tlsTrustStorePassword = getAndRemoveString("tlsTrustStorePassword", "", properties);

        ClientBuilder clientBuilder =
            PulsarClient.builder()
                .loadConf(properties)
                .tlsTrustStorePassword(tlsTrustStorePassword)
                .tlsTrustStorePath(tlsTrustStorePath)
                .tlsTrustCertsFilePath(tlsTrustCertsFilePath)
                .tlsTrustStoreType(tlsTrustStoreType)
                .useKeyStoreTls(useKeyStoreTls)
                .enableTlsHostnameVerification(tlsEnableHostnameVerification)
                .allowTlsInsecureConnection(tlsAllowInsecureConnection)
                .serviceUrl(webServiceUrl)
                .authentication(authentication);
        if (!brokenServiceUrl.isEmpty()) {
          clientBuilder.serviceUrl(brokenServiceUrl);
        }

        pulsarClient = clientBuilder.build();

        pulsarAdmin =
            PulsarAdmin.builder()
                .serviceHttpUrl(webServiceUrl)
                .allowTlsInsecureConnection(tlsAllowInsecureConnection)
                .enableTlsHostnameVerification(tlsEnableHostnameVerification)
                .tlsTrustCertsFilePath(tlsTrustCertsFilePath)
                .useKeyStoreTls(useKeyStoreTls)
                .tlsTrustStoreType(tlsTrustStoreType)
                .tlsTrustStorePath(tlsTrustStorePath)
                .tlsTrustStorePassword(tlsTrustStorePassword)
                .authentication(authentication)
                .build();

      } catch (PulsarClientException err) {
        if (pulsarAdmin != null) {
          pulsarAdmin.close();
        }
        if (pulsarClient != null) {
          pulsarClient.close();
        }
        throw err;
      }
      this.pulsarClient = pulsarClient;
      this.pulsarAdmin = pulsarAdmin;
    } catch (Throwable t) {
      throw Utils.handleException(t);
    }
  }

  private static String getAndRemoveString(
      String name, String defaultValue, Map<String, Object> properties) {
    Object value = (Object) properties.remove(name);
    return value != null ? value.toString() : defaultValue;
  }

  public boolean isEnableClientSideFeatures() {
    return enableClientSideFeatures;
  }

  String getDefaultClientId() {
    return defaultClientId;
  }

  public boolean isEnableTransaction() {
    return enableTransaction;
  }

  public PulsarClient getPulsarClient() {
    return pulsarClient;
  }

  public PulsarAdmin getPulsarAdmin() {
    return pulsarAdmin;
  }

  public String getSystemNamespace() {
    return systemNamespace;
  }

  /**
   * Creates a connection with the default user identity. The connection is created in stopped mode.
   * No messages will be delivered until the {@code Connection.start} method is explicitly called.
   *
   * @return a newly created connection
   * @throws JMSException if the JMS provider fails to create the connection due to some internal
   *     error.
   * @throws JMSSecurityException if client authentication fails due to an invalid user name or
   *     password.
   * @since JMS 1.1
   */
  @Override
  public PulsarConnection createConnection() throws JMSException {
    PulsarConnection res = new PulsarConnection(this);
    connections.add(res);
    return res;
  }

  /**
   * Creates a connection with the specified user identity. The connection is created in stopped
   * mode. No messages will be delivered until the {@code Connection.start} method is explicitly
   * called.
   *
   * @param userName the caller's user name
   * @param password the caller's password
   * @return a newly created connection
   * @throws JMSException if the JMS provider fails to create the connection due to some internal
   *     error.
   * @throws JMSSecurityException if client authentication fails due to an invalid user name or
   *     password.
   * @since JMS 1.1
   */
  @Override
  public PulsarConnection createConnection(String userName, String password) throws JMSException {
    validateDummyUserNamePassword(userName, password);
    return createConnection();
  }

  private void validateDummyUserNamePassword(String userName, String password)
      throws JMSSecurityException {
    if (!tckUsername.isEmpty() && !tckUsername.equals(userName) && !tckPassword.equals(password)) {
      // this verification is here only for the TCK, Pulsar does not support username/password
      // authentication
      // therefore we are using only one single PulsarClient per factory
      // authentication must be set at Factory level
      throw new JMSSecurityException("Unauthorized");
    }
  }

  /**
   * Creates a JMSContext with the default user identity and an unspecified sessionMode.
   *
   * <p>A connection and session are created for use by the new JMSContext. The connection is
   * created in stopped mode but will be automatically started when a JMSConsumer is created.
   *
   * <p>The behaviour of the session that is created depends on whether this method is called in a
   * Java SE environment, in the Java EE application client container, or in the Java EE web or EJB
   * container. If this method is called in the Java EE web or EJB container then the behaviour of
   * the session also depends on whether or not there is an active JTA transaction in progress.
   *
   * <p>In a <b>Java SE environment</b> or in <b>the Java EE application client container</b>:
   *
   * <ul>
   *   <li>The session will be non-transacted and received messages will be acknowledged
   *       automatically using an acknowledgement mode of {@code JMSContext.AUTO_ACKNOWLEDGE} For a
   *       definition of the meaning of this acknowledgement mode see the link below.
   * </ul>
   *
   * <p>In a <b>Java EE web or EJB container, when there is an active JTA transaction in
   * progress</b>:
   *
   * <ul>
   *   <li>The session will participate in the JTA transaction and will be committed or rolled back
   *       when that transaction is committed or rolled back, not by calling the {@code
   *       JMSContext}'s {@code commit} or {@code rollback} methods.
   * </ul>
   *
   * <p>In the <b>Java EE web or EJB container, when there is no active JTA transaction in
   * progress</b>:
   *
   * <ul>
   *   <li>The session will be non-transacted and received messages will be acknowledged
   *       automatically using an acknowledgement mode of {@code JMSContext.AUTO_ACKNOWLEDGE} For a
   *       definition of the meaning of this acknowledgement mode see the link below.
   * </ul>
   *
   * @return a newly created JMSContext
   * @throws JMSRuntimeException if the JMS provider fails to create the JMSContext due to some
   *     internal error.
   * @throws JMSSecurityRuntimeException if client authentication fails due to an invalid user name
   *     or password.
   * @see JMSContext#AUTO_ACKNOWLEDGE
   * @see ConnectionFactory#createContext(int)
   * @see ConnectionFactory#createContext(String, String)
   * @see ConnectionFactory#createContext(String, String, int)
   * @see JMSContext#createContext(int)
   * @since JMS 2.0
   */
  @Override
  public JMSContext createContext() {
    return createContext(JMSContext.AUTO_ACKNOWLEDGE);
  }

  /**
   * Creates a JMSContext with the specified user identity and an unspecified sessionMode.
   *
   * <p>A connection and session are created for use by the new JMSContext. The connection is
   * created in stopped mode but will be automatically started when a JMSConsumer.
   *
   * <p>The behaviour of the session that is created depends on whether this method is called in a
   * Java SE environment, in the Java EE application client container, or in the Java EE web or EJB
   * container. If this method is called in the Java EE web or EJB container then the behaviour of
   * the session also depends on whether or not there is an active JTA transaction in progress.
   *
   * <p>In a <b>Java SE environment</b> or in <b>the Java EE application client container</b>:
   *
   * <ul>
   *   <li>The session will be non-transacted and received messages will be acknowledged
   *       automatically using an acknowledgement mode of {@code JMSContext.AUTO_ACKNOWLEDGE} For a
   *       definition of the meaning of this acknowledgement mode see the link below.
   * </ul>
   *
   * <p>In a <b>Java EE web or EJB container, when there is an active JTA transaction in
   * progress</b>:
   *
   * <ul>
   *   <li>The session will participate in the JTA transaction and will be committed or rolled back
   *       when that transaction is committed or rolled back, not by calling the {@code
   *       JMSContext}'s {@code commit} or {@code rollback} methods.
   * </ul>
   *
   * <p>In the <b>Java EE web or EJB container, when there is no active JTA transaction in
   * progress</b>:
   *
   * <ul>
   *   <li>The session will be non-transacted and received messages will be acknowledged
   *       automatically using an acknowledgement mode of {@code JMSContext.AUTO_ACKNOWLEDGE} For a
   *       definition of the meaning of this acknowledgement mode see the link below.
   * </ul>
   *
   * @param userName the caller's user name
   * @param password the caller's password
   * @return a newly created JMSContext
   * @throws JMSRuntimeException if the JMS provider fails to create the JMSContext due to some
   *     internal error.
   * @throws JMSSecurityRuntimeException if client authentication fails due to an invalid user name
   *     or password.
   * @see JMSContext#AUTO_ACKNOWLEDGE
   * @see ConnectionFactory#createContext()
   * @see ConnectionFactory#createContext(int)
   * @see ConnectionFactory#createContext(String, String, int)
   * @see JMSContext#createContext(int)
   * @since JMS 2.0
   */
  @Override
  public JMSContext createContext(String userName, String password) {
    return createContext(userName, password, JMSContext.AUTO_ACKNOWLEDGE);
  }

  /**
   * Creates a JMSContext with the specified user identity and the specified session mode.
   *
   * <p>A connection and session are created for use by the new JMSContext. The JMSContext is
   * created in stopped mode but will be automatically started when a JMSConsumer is created.
   *
   * <p>The effect of setting the {@code sessionMode} argument depends on whether this method is
   * called in a Java SE environment, in the Java EE application client container, or in the Java EE
   * web or EJB container. If this method is called in the Java EE web or EJB container then the
   * effect of setting the {@code sessionMode} argument also depends on whether or not there is an
   * active JTA transaction in progress.
   *
   * <p>In a <b>Java SE environment</b> or in <b>the Java EE application client container</b>:
   *
   * <ul>
   *   <li>If {@code sessionMode} is set to {@code JMSContext.SESSION_TRANSACTED} then the session
   *       will use a local transaction which may subsequently be committed or rolled back by
   *       calling the {@code JMSContext}'s {@code commit} or {@code rollback} methods.
   *   <li>If {@code sessionMode} is set to any of {@code JMSContext.CLIENT_ACKNOWLEDGE}, {@code
   *       JMSContext.AUTO_ACKNOWLEDGE} or {@code JMSContext.DUPS_OK_ACKNOWLEDGE}. then the session
   *       will be non-transacted and messages received by this session will be acknowledged
   *       according to the value of {@code sessionMode}. For a definition of the meaning of these
   *       acknowledgement modes see the links below.
   * </ul>
   *
   * <p>In a <b>Java EE web or EJB container, when there is an active JTA transaction in
   * progress</b>:
   *
   * <ul>
   *   <li>The argument {@code sessionMode} is ignored. The session will participate in the JTA
   *       transaction and will be committed or rolled back when that transaction is committed or
   *       rolled back, not by calling the {@code JMSContext}'s {@code commit} or {@code rollback}
   *       methods. Since the argument is ignored, developers are recommended to use {@code
   *       createContext(String userName, String password)} instead of this method.
   * </ul>
   *
   * <p>In the <b>Java EE web or EJB container, when there is no active JTA transaction in
   * progress</b>:
   *
   * <ul>
   *   <li>The argument {@code acknowledgeMode} must be set to either of {@code
   *       JMSContext.AUTO_ACKNOWLEDGE} or {@code JMSContext.DUPS_OK_ACKNOWLEDGE}. The session will
   *       be non-transacted and messages received by this session will be acknowledged
   *       automatically according to the value of {@code acknowledgeMode}. For a definition of the
   *       meaning of these acknowledgement modes see the links below. The values {@code
   *       JMSContext.SESSION_TRANSACTED} and {@code JMSContext.CLIENT_ACKNOWLEDGE} may not be used.
   * </ul>
   *
   * @param userName the caller's user name
   * @param password the caller's password
   * @param sessionMode indicates which of four possible session modes will be used.
   *     <ul>
   *       <li>If this method is called in a Java SE environment or in the Java EE application
   *           client container, the permitted values are {@code JMSContext.SESSION_TRANSACTED},
   *           {@code JMSContext.CLIENT_ACKNOWLEDGE}, {@code JMSContext.AUTO_ACKNOWLEDGE} and {@code
   *           JMSContext.DUPS_OK_ACKNOWLEDGE}.
   *       <li>If this method is called in the Java EE web or EJB container when there is an active
   *           JTA transaction in progress then this argument is ignored.
   *       <li>If this method is called in the Java EE web or EJB container when there is no active
   *           JTA transaction in progress, the permitted values are {@code
   *           JMSContext.AUTO_ACKNOWLEDGE} and {@code JMSContext.DUPS_OK_ACKNOWLEDGE}. In this case
   *           the values {@code JMSContext.TRANSACTED} and {@code JMSContext.CLIENT_ACKNOWLEDGE}
   *           are not permitted.
   *     </ul>
   *
   * @return a newly created JMSContext
   * @throws JMSRuntimeException if the JMS provider fails to create the JMSContext due to some
   *     internal error.
   * @throws JMSSecurityRuntimeException if client authentication fails due to an invalid user name
   *     or password.
   * @see JMSContext#SESSION_TRANSACTED
   * @see JMSContext#CLIENT_ACKNOWLEDGE
   * @see JMSContext#AUTO_ACKNOWLEDGE
   * @see JMSContext#DUPS_OK_ACKNOWLEDGE
   * @see ConnectionFactory#createContext()
   * @see ConnectionFactory#createContext(int)
   * @see ConnectionFactory#createContext(String, String)
   * @see JMSContext#createContext(int)
   * @since JMS 2.0
   */
  @Override
  public JMSContext createContext(String userName, String password, int sessionMode) {
    Utils.runtimeException(() -> validateDummyUserNamePassword(userName, password));
    return createContext(sessionMode);
  }

  /**
   * Creates a JMSContext with the default user identity and the specified session mode.
   *
   * <p>A connection and session are created for use by the new JMSContext. The JMSContext is
   * created in stopped mode but will be automatically started when a JMSConsumer is created.
   *
   * <p>The effect of setting the {@code sessionMode} argument depends on whether this method is
   * called in a Java SE environment, in the Java EE application client container, or in the Java EE
   * web or EJB container. If this method is called in the Java EE web or EJB container then the
   * effect of setting the {@code sessionMode} argument also depends on whether or not there is an
   * active JTA transaction in progress.
   *
   * <p>In a <b>Java SE environment</b> or in <b>the Java EE application client container</b>:
   *
   * <ul>
   *   <li>If {@code sessionMode} is set to {@code JMSContext.SESSION_TRANSACTED} then the session
   *       will use a local transaction which may subsequently be committed or rolled back by
   *       calling the {@code JMSContext}'s {@code commit} or {@code rollback} methods.
   *   <li>If {@code sessionMode} is set to any of {@code JMSContext.CLIENT_ACKNOWLEDGE}, {@code
   *       JMSContext.AUTO_ACKNOWLEDGE} or {@code JMSContext.DUPS_OK_ACKNOWLEDGE}. then the session
   *       will be non-transacted and messages received by this session will be acknowledged
   *       according to the value of {@code sessionMode}. For a definition of the meaning of these
   *       acknowledgement modes see the links below.
   * </ul>
   *
   * <p>In a <b>Java EE web or EJB container, when there is an active JTA transaction in
   * progress</b>:
   *
   * <ul>
   *   <li>The argument {@code sessionMode} is ignored. The session will participate in the JTA
   *       transaction and will be committed or rolled back when that transaction is committed or
   *       rolled back, not by calling the {@code JMSContext}'s {@code commit} or {@code rollback}
   *       methods. Since the argument is ignored, developers are recommended to use {@code
   *       createContext()} instead of this method.
   * </ul>
   *
   * <p>In the <b>Java EE web or EJB container, when there is no active JTA transaction in
   * progress</b>:
   *
   * <ul>
   *   <li>The argument {@code acknowledgeMode} must be set to either of {@code
   *       JMSContext.AUTO_ACKNOWLEDGE} or {@code JMSContext.DUPS_OK_ACKNOWLEDGE}. The session will
   *       be non-transacted and messages received by this session will be acknowledged
   *       automatically according to the value of {@code acknowledgeMode}. For a definition of the
   *       meaning of these acknowledgement modes see the links below. The values {@code
   *       JMSContext.SESSION_TRANSACTED} and {@code JMSContext.CLIENT_ACKNOWLEDGE} may not be used.
   * </ul>
   *
   * @param sessionMode indicates which of four possible session modes will be used.
   *     <ul>
   *       <li>If this method is called in a Java SE environment or in the Java EE application
   *           client container, the permitted values are {@code JMSContext.SESSION_TRANSACTED},
   *           {@code JMSContext.CLIENT_ACKNOWLEDGE}, {@code JMSContext.AUTO_ACKNOWLEDGE} and {@code
   *           JMSContext.DUPS_OK_ACKNOWLEDGE}.
   *       <li>If this method is called in the Java EE web or EJB container when there is an active
   *           JTA transaction in progress then this argument is ignored.
   *       <li>If this method is called in the Java EE web or EJB container when there is no active
   *           JTA transaction in progress, the permitted values are {@code
   *           JMSContext.AUTO_ACKNOWLEDGE} and {@code JMSContext.DUPS_OK_ACKNOWLEDGE}. In this case
   *           the values {@code JMSContext.TRANSACTED} and {@code JMSContext.CLIENT_ACKNOWLEDGE}
   *           are not permitted.
   *     </ul>
   *
   * @return a newly created JMSContext
   * @throws JMSRuntimeException if the JMS provider fails to create the JMSContext due to some
   *     internal error.
   * @throws JMSSecurityRuntimeException if client authentication fails due to an invalid user name
   *     or password.
   * @see JMSContext#SESSION_TRANSACTED
   * @see JMSContext#CLIENT_ACKNOWLEDGE
   * @see JMSContext#AUTO_ACKNOWLEDGE
   * @see JMSContext#DUPS_OK_ACKNOWLEDGE
   * @see ConnectionFactory#createContext()
   * @see ConnectionFactory#createContext(String, String)
   * @see ConnectionFactory#createContext(String, String, int)
   * @see JMSContext#createContext(int)
   * @since JMS 2.0
   */
  @Override
  public JMSContext createContext(int sessionMode) {
    return new PulsarJMSContext(this, sessionMode);
  }

  public void close() {
    // close all connections and wait for ongoing operations
    // to complete
    for (PulsarConnection con : new ArrayList<>(connections)) {
      try {
        con.close();
      } catch (Exception ignore) {
        // ignore
        Utils.handleException(ignore);
      }
    }

    for (Producer<?> producer : producers.values()) {
      try {
        producer.close();
      } catch (PulsarClientException ignore) {
        // ignore
        Utils.handleException(ignore);
      }
    }

    this.pulsarAdmin.close();

    try {
      this.pulsarClient.close();
    } catch (PulsarClientException err) {
      log.info("Error closing PulsarClient", err);
    }
  }

  Producer<byte[]> getProducerForDestination(
      PulsarDestination defaultDestination, boolean transactions) throws JMSException {
    try {
      return producers.computeIfAbsent(
          defaultDestination.topicName + "-" + transactions,
          d -> {
            try {
              return Utils.invoke(
                  () -> {
                    ProducerBuilder<byte[]> producerBuilder =
                        pulsarClient
                            .newProducer()
                            .topic(defaultDestination.topicName)
                            .loadConf(producerConfiguration);
                    if (transactions) {
                      // this is a limitation of Pulsar transaction support
                      producerBuilder.sendTimeout(0, TimeUnit.MILLISECONDS);
                    }
                    return producerBuilder.create();
                  });
            } catch (JMSException err) {
              throw new RuntimeException(err);
            }
          });
    } catch (RuntimeException err) {
      throw (JMSException) err.getCause();
    }
  }

  public void ensureSubscription(PulsarDestination destination, String consumerName)
      throws JMSException {
    // for queues we have a single shared subscription
    String subscriptionName = destination.isQueue() ? queueSubscriptioName : consumerName;
    log.info("Creating subscription {} for destination {}", subscriptionName, destination);
    try {
      pulsarAdmin
          .topics()
          .createSubscription(destination.topicName, subscriptionName, MessageId.latest);
    } catch (PulsarAdminException.ConflictException alreadyExists) {
      log.info("Subscription {} already exists, this is usually not a problem", subscriptionName);
    } catch (Exception err) {
      throw Utils.handleException(err);
    }
  }

  public Consumer<byte[]> createConsumer(
      PulsarDestination destination,
      String consumerName,
      int sessionMode,
      SubscriptionMode subscriptionMode,
      SubscriptionType subscriptionType)
      throws JMSException {
    // for queues we have a single shared subscription
    String subscriptionName = destination.isQueue() ? queueSubscriptioName : consumerName;
    SubscriptionInitialPosition initialPosition =
        destination.isTopic()
            ? SubscriptionInitialPosition.Latest
            : SubscriptionInitialPosition.Earliest;
    MessageId seekMessageId = null;
    if (destination.isQueue() && subscriptionMode != SubscriptionMode.Durable) {
      throw new IllegalStateException("only durable mode for queues");
    }
    if (destination.isQueue() && subscriptionType != SubscriptionType.Shared) {
      throw new IllegalStateException("only Shared SubscriptionType for queues");
    }

    log.debug(
        "createConsumer {} {} {}",
        destination.topicName,
        consumerName,
        subscriptionMode,
        subscriptionType);

    try {
      ConsumerBuilder<byte[]> builder =
          pulsarClient
              .newConsumer()
              // these properties can be overridden by the configuration
              .negativeAckRedeliveryDelay(1, TimeUnit.SECONDS)
              .loadConf(consumerConfiguration)
              // these properties cannot be overwritten by the configuration
              .subscriptionInitialPosition(initialPosition)
              .subscriptionMode(subscriptionMode)
              .subscriptionType(subscriptionType)
              .subscriptionName(subscriptionName)
              .topic(destination.topicName);
      Consumer<byte[]> newConsumer = builder.subscribe();
      consumers.add(newConsumer);
      return newConsumer;
    } catch (PulsarClientException err) {
      throw Utils.handleException(err);
    }
  }

  public Reader<byte[]> createReaderForBrowser(PulsarQueue destination) throws JMSException {

    try {
      List<Message<byte[]>> messages =
          getPulsarAdmin().topics().peekMessages(destination.topicName, queueSubscriptioName, 1);

      MessageId seekMessageId;
      if (messages.isEmpty()) {
        // no more messages
        seekMessageId = MessageId.latest;
      } else {
        seekMessageId = messages.get(0).getMessageId();
      }
      ;
      log.info("createBrowser {} at {}", destination.topicName, seekMessageId);

      ReaderBuilder<byte[]> builder =
          pulsarClient
              .newReader()
              // these properties can be overridden by the configuration
              .loadConf(consumerConfiguration)
              // these properties cannot be overwritten by the configuration
              .readerName("jms-queue-browser-" + UUID.randomUUID())
              .startMessageId(seekMessageId)
              .startMessageIdInclusive()
              .topic(destination.topicName);
      Reader<byte[]> newReader = builder.create();
      readers.add(newReader);
      return newReader;
    } catch (PulsarClientException | PulsarAdminException err) {
      throw Utils.handleException(err);
    }
  }

  public void removeConsumer(Consumer<byte[]> consumer) {
    consumers.remove(consumer);
  }

  public void removeReader(Reader<byte[]> reader) {
    readers.remove(reader);
  }

  public boolean deleteSubscription(PulsarDestination destination, String name)
      throws JMSException {
    boolean somethingDone = false;
    try {

      // TCK mode, scan for all subscriptions
      List<String> allTopics = pulsarAdmin.topics().getList(systemNamespace);
      for (String topic : allTopics) {
        log.info("Scanning topic {}", topic);
        List<String> subscriptions = pulsarAdmin.topics().getSubscriptions(topic);
        log.info("Subscriptions {}", subscriptions);
        for (String subscription : subscriptions) {
          log.info("Found subscription {} ", subscription);
          if (subscription.equals(name)) {
            log.info("deleteSubscription topic {} name {}", topic, name);
            pulsarAdmin.topics().deleteSubscription(topic, name, true);
            somethingDone = true;
          }
        }
      }

      if (destination != null) {
        log.info("deleteSubscription topic {} name {}", destination.topicName, name);
        pulsarAdmin.topics().deleteSubscription(destination.topicName, name, true);
        somethingDone = true;
      }
    } catch (PulsarAdminException.NotFoundException notFound) {
      log.error("Cannot unsubscribe {} from {}: not found", name, destination.topicName);
    } catch (Exception err) {
      throw Utils.handleException(err);
    }
    return somethingDone;
  }

  public void registerClientId(String clientID) throws InvalidClientIDException {
    log.info("registerClientId {}, existing {}", clientID, clientIdentifiers);
    if (!clientIdentifiers.add(clientID)) {
      throw new InvalidClientIDException(
          "A connection with this client id '" + clientID + "'is already opened locally");
    }
  }

  public void unregisterConnection(PulsarConnection connection) {
    if (connection.clientId != null) {
      clientIdentifiers.remove(connection.clientId);
      log.info("unregisterClientId {} {}", connection.clientId, clientIdentifiers);
    }
    connections.remove(connection);
  }

  @Override
  public QueueConnection createQueueConnection() throws JMSException {
    return createConnection();
  }

  @Override
  public QueueConnection createQueueConnection(String s, String s1) throws JMSException {
    return createConnection(s, s1);
  }

  @Override
  public TopicConnection createTopicConnection() throws JMSException {
    return createConnection();
  }

  @Override
  public TopicConnection createTopicConnection(String s, String s1) throws JMSException {
    return createConnection(s, s1);
  }

  public boolean isForceDeleteTemporaryDestinations() {
    return forceDeleteTemporaryDestinations;
  }

  public String getQueueSubscriptionName() {
    return queueSubscriptioName;
  }

  public SubscriptionType getExclusiveSubscriptionTypeForSimpleConsumers() {
    return useExclusiveSubscriptionsForSimpleConsumers
        ? SubscriptionType.Exclusive
        : SubscriptionType.Shared;
  }
}
