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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import javax.jms.*;
import javax.jms.IllegalStateException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.BatcherBuilder;
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
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.SubscriptionStats;

@Slf4j
public class PulsarConnectionFactory
    implements ConnectionFactory,
        QueueConnectionFactory,
        TopicConnectionFactory,
        AutoCloseable,
        Serializable {

  private static final long serialVersionUID = 1231231L;

  private static final String PENDING_ACK_STORE_SUFFIX = "__transaction_pending_ack";
  private static final String SHADED_PREFIX = "com.datastax.oss.pulsar.jms.shaded.";
  private static final boolean NEEDS_RELOCATION =
      PulsarClient.class.getName().startsWith(SHADED_PREFIX);

  private static final Set<String> clientIdentifiers = new ConcurrentSkipListSet<>();

  // see resetDefaultValues for final fields
  private final transient Map<String, Producer<byte[]>> producers = new ConcurrentHashMap<>();
  private final transient Set<PulsarConnection> connections =
      Collections.synchronizedSet(new HashSet<>());
  private final transient List<Consumer<?>> consumers = new CopyOnWriteArrayList<>();
  private final transient List<Reader<?>> readers = new CopyOnWriteArrayList<>();

  private transient PulsarClient pulsarClient;
  private transient PulsarAdmin pulsarAdmin;
  private transient Map<String, Object> producerConfiguration;
  private transient Map<String, Object> consumerConfiguration;
  private transient Schema<?> consumerSchema;
  private transient String systemNamespace = "public/default";
  private transient String defaultClientId = null;
  private transient boolean enableTransaction = false;
  private transient boolean emulateTransactions = false;
  private transient boolean enableClientSideEmulation = false;
  private transient boolean useServerSideFiltering = false;
  private transient boolean forceDeleteTemporaryDestinations = false;
  private transient boolean useExclusiveSubscriptionsForSimpleConsumers = false;
  private transient boolean acknowledgeRejectedMessages = false;
  private transient String tckUsername = "";
  private transient String tckPassword = "";
  private transient boolean useCredentialsFromCreateConnection = false;
  private transient String lastConnectUsername = null;
  private transient String lastConnectPassword = null;
  private transient String queueSubscriptionName = "jms-queue";
  private transient SubscriptionType topicSharedSubscriptionType = SubscriptionType.Shared;
  private transient long waitForServerStartupTimeout = 60000;
  private transient boolean usePulsarAdmin = true;
  private transient boolean precreateQueueSubscription = true;
  private transient int precreateQueueSubscriptionConsumerQueueSize = 0;
  private transient boolean initialized;
  private transient boolean closed;

  private transient Map<String, Object> configuration = Collections.emptyMap();

  public PulsarConnectionFactory() throws JMSException {
    this(new HashMap<>());
  }

  public PulsarConnectionFactory(Map<String, Object> properties) {
    setConfiguration(properties);
  }

  public PulsarConnectionFactory(String configuration) throws JMSException {
    this();
    setJsonConfiguration(configuration);
  }

  /**
   * Utility method for configuration based on JSON
   *
   * @return JSON encoded configuration
   */
  public String getJsonConfiguration() {
    return Utils.runtimeException(() -> new ObjectMapper().writeValueAsString(getConfiguration()));
  }

  /**
   * Apply configuration from a JSON encoded string
   *
   * @param json the JSON
   */
  public void setJsonConfiguration(String json) {
    if (json == null || json.isEmpty()) {
      setConfiguration(Collections.emptyMap());
      return;
    }
    setConfiguration(Utils.runtimeException(() -> new ObjectMapper().readValue(json, Map.class)));
  }

  public synchronized Map<String, Object> getConfiguration() {
    return Utils.deepCopyMap(configuration);
  }

  public synchronized void setConfiguration(Map<String, Object> configuration) {
    this.configuration = copyAndApplyShadedPrefix(configuration);
  }

  private static Map<String, Object> copyAndApplyShadedPrefix(Map<String, Object> configuration) {
    if (configuration == null) {
      return null;
    }
    if (!NEEDS_RELOCATION) {
      return new HashMap<>(configuration);
    }
    Map<String, Object> copy = new HashMap<>();
    configuration.forEach(
        (key, value) -> {
          if (value instanceof Map) {
            copy.put(key, copyAndApplyShadedPrefix((Map) value));
            return;
          }
          if (value instanceof String) {
            String result = (String) value;
            if (result.length() > 17
                && result
                    .substring(1)
                    .startsWith("rg.apache.pulsar") // hack to deal with the Maven Shade plugin
            ) {
              result = SHADED_PREFIX + value;
            }
            if (log.isDebugEnabled()) {
              log.debug("Relocating {} = {} -> {}", key, value, result);
            }
            copy.put(key, result);
            return;
          }
          copy.put(key, value);
        });
    return copy;
  }

  private synchronized Map<String, Object> getConsumerConfiguration() {
    return consumerConfiguration;
  }

  private synchronized Schema<?> getConsumerSchema() {
    return consumerSchema;
  }

  private synchronized Map<String, Object> getProducerConfiguration() {
    return producerConfiguration;
  }

  private synchronized void ensureInitialized(String connectUsername, String connectPassword)
      throws JMSException {
    if (initialized) {
      return;
    }
    if (closed) {
      throw new IllegalStateException("This ConnectionFactory is closed");
    }
    Map<String, Object> configurationCopy = Utils.deepCopyMap(this.configuration);
    try {

      Map<String, Object> producerConfiguration =
          (Map<String, Object>) configurationCopy.remove("producerConfig");
      if (producerConfiguration != null) {
        Object batcherBuilder = producerConfiguration.get("batcherBuilder");
        if (batcherBuilder != null) {
          if (batcherBuilder instanceof String) {
            String batcherBuilderString = (String) batcherBuilder;
            BatcherBuilder builder = BatcherBuilder.DEFAULT;
            switch (batcherBuilderString) {
              case "KEY_BASED":
                builder = BatcherBuilder.KEY_BASED;
                break;
              case "DEFAULT":
                builder = BatcherBuilder.DEFAULT;
                break;
              default:
                throw new IllegalArgumentException(
                    "Unsupported batcherBuilder " + batcherBuilderString);
            }
            producerConfiguration.put("batcherBuilder", builder);
          }
        }
        this.producerConfiguration = new HashMap(producerConfiguration);
      } else {
        this.producerConfiguration = Collections.emptyMap();
      }

      this.consumerSchema = Schema.BYTES;
      Map<String, Object> consumerConfigurationM =
          (Map<String, Object>) configurationCopy.remove("consumerConfig");
      if (consumerConfigurationM != null) {
        this.consumerConfiguration = new HashMap(consumerConfigurationM);
        boolean useSchema =
            Boolean.parseBoolean(getAndRemoveString("useSchema", "false", consumerConfiguration));
        if (useSchema) {
          consumerSchema = Schema.AUTO_CONSUME();
        }
      } else {
        this.consumerConfiguration = Collections.emptyMap();
      }
      this.systemNamespace =
          getAndRemoveString("jms.systemNamespace", "public/default", configurationCopy);

      this.tckUsername = getAndRemoveString("jms.tckUsername", "", configurationCopy);
      this.tckPassword = getAndRemoveString("jms.tckPassword", "", configurationCopy);

      // with useCredentialsFromCreateConnection we pick the username/password from
      // Connection.connect() to
      // configure authentication parameters.
      // the meaning of username/password depends on the Authentication Plugin
      this.useCredentialsFromCreateConnection =
          Boolean.parseBoolean(
              getAndRemoveString(
                  "jms.useCredentialsFromCreateConnection", "false", configurationCopy));

      this.defaultClientId = getAndRemoveString("jms.clientId", null, configurationCopy);

      this.queueSubscriptionName =
          getAndRemoveString("jms.queueSubscriptionName", "jms-queue", configurationCopy);

      this.usePulsarAdmin =
          Boolean.parseBoolean(getAndRemoveString("jms.usePulsarAdmin", "true", configurationCopy));

      this.precreateQueueSubscription =
          Boolean.parseBoolean(
              getAndRemoveString("jms.precreateQueueSubscription", "true", configurationCopy));

      this.precreateQueueSubscriptionConsumerQueueSize =
          Integer.parseInt(
              getAndRemoveString(
                  "jms.precreateQueueSubscriptionConsumerQueueSize", "0", configurationCopy));

      final String rawTopicSharedSubscriptionType =
          getAndRemoveString(
              "jms.topicSharedSubscriptionType", SubscriptionType.Shared.name(), configurationCopy);
      this.topicSharedSubscriptionType =
          Stream.of(SubscriptionType.values())
              .filter(
                  t ->
                      t.name().equalsIgnoreCase(rawTopicSharedSubscriptionType)
                          && t != SubscriptionType.Exclusive)
              .findFirst()
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "Invalid jms.topicSubscriptionType: "
                              + rawTopicSharedSubscriptionType
                              + ", only "
                              + SubscriptionType.Shared
                              + ", "
                              + SubscriptionType.Key_Shared
                              + " and "
                              + SubscriptionType.Failover
                              + " "));

      this.waitForServerStartupTimeout =
          Long.parseLong(
              getAndRemoveString("jms.waitForServerStartupTimeout", "60000", configurationCopy));

      this.enableClientSideEmulation =
          Boolean.parseBoolean(
              getAndRemoveString("jms.enableClientSideEmulation", "false", configurationCopy));

      this.useServerSideFiltering =
          Boolean.parseBoolean(
              getAndRemoveString("jms.useServerSideFiltering", "false", configurationCopy));

      // in Exclusive mode Pulsar does not support delayed messages
      // with this flag you force to not use Exclusive subscription and so to support
      // delayed messages are well
      this.useExclusiveSubscriptionsForSimpleConsumers =
          Boolean.parseBoolean(
              getAndRemoveString(
                  "jms.useExclusiveSubscriptionsForSimpleConsumers", "true", configurationCopy));

      // This flag is to force acknowledgement for messages that are rejected due to
      // client side filtering in case of Shared subscription.
      // If you have a shared subscription on a topic (Topic or Queue) and a message
      // is filtered out, by default we negatively acknowledge the message in order to
      // let another consumer on the same subscription to receive it.
      // with this flag turned to "true" when a Consumer receives a message and it filters
      // it out, we acknowledge the message, this way it won't be consumed anymore.
      this.acknowledgeRejectedMessages =
          Boolean.parseBoolean(
              getAndRemoveString("jms.acknowledgeRejectedMessages", "false", configurationCopy));
      // default is false
      this.forceDeleteTemporaryDestinations =
          Boolean.parseBoolean(
              getAndRemoveString(
                  "jms.forceDeleteTemporaryDestinations", "false", configurationCopy));

      this.enableTransaction =
          Boolean.parseBoolean(
              configurationCopy.getOrDefault("enableTransaction", "false").toString());

      this.emulateTransactions =
          Boolean.parseBoolean(
              getAndRemoveString("jms.emulateTransactions", "false", configurationCopy).toString());

      if (emulateTransactions && enableTransaction) {
        throw new IllegalStateException(
            "You cannot set both enableTransaction and jms.emulateTransactions");
      }

      String webServiceUrl =
          getAndRemoveString("webServiceUrl", "http://localhost:8080", configurationCopy);

      String brokenServiceUrl = getAndRemoveString("brokerServiceUrl", "", configurationCopy);

      PulsarClient pulsarClient = null;
      PulsarAdmin pulsarAdmin = null;
      try {

        // must be the same as
        // https://pulsar.apache.org/docs/en/security-tls-keystore/#configuring-clients
        String authPluginClassName = getAndRemoveString("authPlugin", "", configurationCopy);
        String authParamsString = getAndRemoveString("authParams", "", configurationCopy);

        if (useCredentialsFromCreateConnection) {
          if (connectUsername == null) {
            connectUsername = "";
          }
          if (connectPassword == null) {
            connectPassword = "";
          }
          // for JWT token authentication the "password" is passed as "authParams"
          if (authPluginClassName.equals(AuthenticationToken.class.getName())) {
            authParamsString = connectPassword;
          } else {
            throw new javax.jms.IllegalStateRuntimeException(
                "With jms.useCredentialsFromConnect:true "
                    + "only JWT (AuthenticationToken) authentication is currently supported");
          }
        }

        Authentication authentication =
            AuthenticationFactory.create(authPluginClassName, authParamsString);
        if (log.isDebugEnabled()) {
          log.debug("Authentication {}", authentication);
        }
        boolean tlsAllowInsecureConnection =
            Boolean.parseBoolean(
                getAndRemoveString("tlsAllowInsecureConnection", "false", configurationCopy));

        boolean tlsEnableHostnameVerification =
            Boolean.parseBoolean(
                getAndRemoveString("tlsEnableHostnameVerification", "false", configurationCopy));
        final String tlsTrustCertsFilePath =
            (String) getAndRemoveString("tlsTrustCertsFilePath", "", configurationCopy);

        boolean useKeyStoreTls =
            Boolean.parseBoolean(getAndRemoveString("useKeyStoreTls", "false", configurationCopy));
        String tlsTrustStoreType =
            getAndRemoveString("tlsTrustStoreType", "JKS", configurationCopy);
        String tlsTrustStorePath = getAndRemoveString("tlsTrustStorePath", "", configurationCopy);
        String tlsTrustStorePassword =
            getAndRemoveString("tlsTrustStorePassword", "", configurationCopy);

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

        ClientBuilder clientBuilder =
            PulsarClient.builder()
                .loadConf(configurationCopy)
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

      if (useCredentialsFromCreateConnection) {
        // commit credentials only in case of success
        this.lastConnectUsername = connectUsername;
        this.lastConnectPassword = connectPassword;
      }
      this.initialized = true;
    } catch (Throwable t) {
      throw Utils.handleException(t);
    }
  }

  private void validateConnectUsernamePasswordReused(String connectUsername, String connectPassword)
      throws IllegalStateException {
    if (lastConnectUsername != null) {
      if (!Objects.equals(connectUsername, lastConnectUsername)) {
        throw new IllegalStateException(
            "With jms.useCredentialsFromConnect:true "
                + "once you call connect(username,password) you must always use the same credentials, "
                + "bad username "
                + connectUsername
                + ", expecting "
                + lastConnectUsername);
      }
      if (!Objects.equals(connectPassword, lastConnectPassword)) {
        throw new IllegalStateException(
            "With jms.useCredentialsFromConnect:true "
                + "once you call connect(username,password) you must always use the same credentials, "
                + "password does not match");
      }
    }
  }

  private static String getAndRemoveString(
      String name, String defaultValue, Map<String, Object> properties) {
    Object value = (Object) properties.remove(name);
    return value != null ? value.toString() : defaultValue;
  }

  public synchronized boolean isEnableClientSideEmulation() {
    return enableClientSideEmulation;
  }

  public synchronized boolean isUseServerSideFiltering() {
    return useServerSideFiltering;
  }

  synchronized String getDefaultClientId() {
    return defaultClientId;
  }

  public synchronized boolean isEnableTransaction() {
    return enableTransaction;
  }

  public synchronized boolean isEmulateTransactions() {
    return emulateTransactions;
  }

  public synchronized PulsarClient getPulsarClient() {
    return pulsarClient;
  }

  public synchronized PulsarAdmin getPulsarAdmin() throws javax.jms.IllegalStateException {
    if (!usePulsarAdmin) {
      throw new javax.jms.IllegalStateException(
          "jms.usePulsarAdmin is set to false, this feature is not available");
    }
    return pulsarAdmin;
  }

  public synchronized String getSystemNamespace() {
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
    return createConnection(null, null);
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
    validateUserNamePassword(userName, password);
    ensureInitialized(userName, password);
    PulsarConnection res = new PulsarConnection(this);
    connections.add(res);
    return res;
  }

  private synchronized void validateUserNamePassword(String userName, String password)
      throws JMSException {
    if (useCredentialsFromCreateConnection) {
      validateConnectUsernamePasswordReused(userName, password);
    }
    if (tckUsername != null
        && !tckUsername.isEmpty()
        && !tckUsername.equals(userName)
        && tckPassword != null
        && !tckPassword.equals(password)) {
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
    Utils.runtimeException(() -> validateUserNamePassword(userName, password));
    Utils.runtimeException(() -> ensureInitialized(userName, password));
    return new PulsarJMSContext(this, sessionMode, userName, password);
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
    return createContext(null, null, sessionMode);
  }

  public void close() {
    synchronized (this) {
      closed = true;
      if (!initialized) {
        return;
      }
    }
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

  public static PulsarDestination toPulsarDestination(Destination destination) throws JMSException {
    if (destination instanceof PulsarDestination) {
      return (PulsarDestination) destination;
    } else if (destination instanceof Queue) {
      return new PulsarQueue(((Queue) destination).getQueueName());
    } else if (destination instanceof Topic) {
      return new PulsarTopic(((Topic) destination).getTopicName());
    } else {
      throw new IllegalStateException("Cannot convert " + destination + " to a PulsarDestination");
    }
  }

  public String getPulsarTopicName(Destination defaultDestination) throws JMSException {
    PulsarDestination destination = toPulsarDestination(defaultDestination);
    String topicName = destination.getInternalTopicName();
    return applySystemNamespace(topicName);
  }

  Producer<byte[]> getProducerForDestination(Destination defaultDestination, boolean transactions)
      throws JMSException {
    try {
      String fullQualifiedTopicName = getPulsarTopicName(defaultDestination);
      String key = transactions ? fullQualifiedTopicName + "-tx" : fullQualifiedTopicName;
      return producers.computeIfAbsent(
          key,
          d -> {
            try {
              return Utils.invoke(
                  () -> {
                    Map<String, Object> producerConfiguration = getProducerConfiguration();
                    ProducerBuilder<byte[]> producerBuilder =
                        pulsarClient
                            .newProducer()
                            .topic(applySystemNamespace(fullQualifiedTopicName))
                            .loadConf(producerConfiguration);
                    if (producerConfiguration.containsKey("batcherBuilder")) {
                      producerBuilder.batcherBuilder(
                          (BatcherBuilder) producerConfiguration.get("batcherBuilder"));
                    }
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

  synchronized boolean isUsePulsarAdmin() {
    return usePulsarAdmin;
  }

  synchronized boolean isPrecreateQueueSubscription() {
    return precreateQueueSubscription;
  }

  public void ensureQueueSubscription(PulsarDestination destination) throws JMSException {
    if (!isPrecreateQueueSubscription()) {
      return;
    }
    long start = System.currentTimeMillis();

    // please note that in the special jms-queue subscription we cannot
    // set a selector, because it is shared among all the Consumers of the Queue
    String fullQualifiedTopicName = getPulsarTopicName(destination);
    while (true) {
      try {
        if (isUsePulsarAdmin()) {
          getPulsarAdmin()
              .topics()
              .createSubscription(
                  fullQualifiedTopicName,
                  getQueueSubscriptionName(destination),
                  MessageId.earliest);
        } else {
          // if we cannot use PulsarAdmin or we want to set a selector,
          // let's try to create a consumer with zero queue
          getPulsarClient()
              .newConsumer()
              .subscriptionType(getTopicSharedSubscriptionType())
              .subscriptionName(getQueueSubscriptionName(destination))
              .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
              .receiverQueueSize(getPrecreateQueueSubscriptionConsumerQueueSize())
              .topic(fullQualifiedTopicName)
              .subscribe()
              .close();
        }
        break;
      } catch (PulsarAdminException.ConflictException exists) {
        log.debug(
            "Subscription {} already exists for {}",
            getQueueSubscriptionName(destination),
            fullQualifiedTopicName);
        break;
      } catch (PulsarAdminException | PulsarClientException err) {
        // special handling for server startup
        // it mitigates problems in tests
        // but also it is useful in order to let
        // applications start when the server is not available
        long now = System.currentTimeMillis();
        if (now - start > getWaitForServerStartupTimeout()) {
          throw Utils.handleException(err);
        } else {
          log.info(
              "Got {} error while setting up subscription for queue {}, maybe the namespace/broker is still starting",
              err.toString(),
              fullQualifiedTopicName);

          try {
            Thread.sleep(1000);
          } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw Utils.handleException(err);
          }
        }
      }
    }
  }

  public Consumer<?> createConsumer(
      PulsarDestination destination,
      String consumerName,
      int sessionMode,
      SubscriptionMode subscriptionMode,
      SubscriptionType subscriptionType,
      String messageSelector,
      boolean noLocal,
      String jmsConnectionID,
      AtomicReference<String> selectorOnSubscriptionReceiver)
      throws JMSException {
    String fullQualifiedTopicName = getPulsarTopicName(destination);
    // for queues we have a single shared subscription
    String subscriptionName =
        destination.isQueue() ? getQueueSubscriptionName(destination) : consumerName;
    SubscriptionInitialPosition initialPosition =
        destination.isTopic()
            ? SubscriptionInitialPosition.Latest
            : SubscriptionInitialPosition.Earliest;
    if (destination.isQueue() && subscriptionMode != SubscriptionMode.Durable) {
      throw new IllegalStateException("only durable mode for queues");
    }
    if (destination.isQueue() && subscriptionType == SubscriptionType.Exclusive) {
      throw new IllegalStateException("only Shared SubscriptionType for queues");
    }

    log.debug(
        "createConsumer {} {} {} {}",
        fullQualifiedTopicName,
        consumerName,
        subscriptionMode,
        subscriptionType,
        messageSelector);
    Map<String, String> subscriptionProperties = new HashMap<>();
    Map<String, String> consumerMetadata = new HashMap<>();
    consumerMetadata.put("jms.destination.type", destination.isQueue() ? "queue" : "topic");
    if (isUseServerSideFiltering()) {
      // this flag enables filtering on the subscription/consumer
      // the plugin will apply filtering only on these subscriptions/consumers,
      // in order to not impact on other subscriptions
      consumerMetadata.put("jms.filtering", "true");
      subscriptionProperties.put("jms.destination.type", destination.isQueue() ? "queue" : "topic");
      if (noLocal) {
        consumerMetadata.put("jms.filter.JMSConnectionID", jmsConnectionID);
      }
    }
    if (isUseServerSideFiltering()) {
      if (messageSelector != null) {
        consumerMetadata.put("jms.selector", messageSelector);
      }
      if (destination.isTopic()) {
        consumerMetadata.put("jms.selector.reject.action", "drop");
      } else {
        // for Queue is it on the Consumer
        consumerMetadata.put("jms.selector.reject.action", "reschedule");
      }
    }
    if (isAcknowledgeRejectedMessages()) {
      consumerMetadata.put("jms.force.drop.rejected", "true");
    }

    try {
      if (isUseServerSideFiltering() && subscriptionMode == SubscriptionMode.Durable) {
        try {
          Map<String, ? extends SubscriptionStats> subscriptions =
              pulsarAdmin.topics().getStats(fullQualifiedTopicName).getSubscriptions();
          SubscriptionStats subscriptionStats = subscriptions.get(subscriptionName);
          if (subscriptionStats != null) {
            Map<String, String> subscriptionPropertiesFromBroker =
                subscriptionStats.getSubscriptionProperties();
            log.info("subscriptionPropertiesFromBroker {}", subscriptionPropertiesFromBroker);
            if (subscriptionPropertiesFromBroker != null) {
              boolean filtering =
                  "true".equals(subscriptionPropertiesFromBroker.get("jms.filtering"));
              if (filtering) {
                String selectorOnSubscription =
                    subscriptionPropertiesFromBroker.getOrDefault("jms.selector", "");
                if (!selectorOnSubscription.isEmpty()) {
                  log.info(
                      "Detected selector {} on Subscription {} on topic {}",
                      selectorOnSubscription,
                      subscriptionName,
                      fullQualifiedTopicName);
                  selectorOnSubscriptionReceiver.set(selectorOnSubscription);
                }
              }
            }
          }
        } catch (PulsarAdminException.NotFoundException notFoundException) {
        }
      }

      Schema<?> schema = getConsumerSchema();
      Map<String, Object> consumerConfiguration = getConsumerConfiguration();
      ConsumerBuilder<?> builder =
          pulsarClient
              .newConsumer(schema)
              // these properties can be overridden by the configuration
              .negativeAckRedeliveryDelay(1, TimeUnit.SECONDS)
              .loadConf(consumerConfiguration)
              .properties(consumerMetadata)
              // these properties cannot be overwritten by the configuration
              .subscriptionInitialPosition(initialPosition)
              .subscriptionMode(subscriptionMode)
              .subscriptionProperties(subscriptionProperties)
              .subscriptionType(subscriptionType)
              .subscriptionName(subscriptionName)
              .topic(fullQualifiedTopicName);
      Consumer<?> newConsumer = builder.subscribe();
      consumers.add(newConsumer);

      return newConsumer;
    } catch (PulsarClientException | PulsarAdminException err) {
      throw Utils.handleException(err);
    }
  }

  public Reader<?> createReaderForBrowser(PulsarQueue destination) throws JMSException {
    String fullQualifiedTopicName = getPulsarTopicName(destination);
    try {
      List<Message<byte[]>> messages =
          getPulsarAdmin()
              .topics()
              .peekMessages(fullQualifiedTopicName, getQueueSubscriptionName(destination), 1);

      MessageId seekMessageId;
      if (messages.isEmpty()) {
        // no more messages
        seekMessageId = MessageId.latest;
      } else {
        seekMessageId = messages.get(0).getMessageId();
      }
      if (log.isDebugEnabled()) {
        log.debug("createBrowser {} at {}", fullQualifiedTopicName, seekMessageId);
      }
      Schema<?> schema = getConsumerSchema();
      ReaderBuilder<?> builder =
          pulsarClient
              .newReader(schema)
              // these properties can be overridden by the configuration
              .loadConf(getConsumerConfiguration())
              // these properties cannot be overwritten by the configuration
              .readerName("jms-queue-browser-" + UUID.randomUUID())
              .startMessageId(seekMessageId)
              .startMessageIdInclusive()
              .topic(fullQualifiedTopicName);
      Reader<?> newReader = builder.create();
      readers.add(newReader);
      return newReader;
    } catch (PulsarClientException | PulsarAdminException err) {
      throw Utils.handleException(err);
    }
  }

  public void removeConsumer(Consumer<?> consumer) {
    consumers.remove(consumer);
  }

  public void removeReader(Reader<?> reader) {
    readers.remove(reader);
  }

  public boolean deleteSubscription(PulsarDestination destination, String name)
      throws JMSException {
    String systemNamespace = getSystemNamespace();
    boolean somethingDone = false;
    try {

      if (destination != null) {
        String fullQualifiedTopicName = getPulsarTopicName(destination);
        log.info("deleteSubscription topic {} name {}", fullQualifiedTopicName, name);
        try {
          pulsarAdmin.topics().deleteSubscription(fullQualifiedTopicName, name, true);
          somethingDone = true;
        } catch (PulsarAdminException.NotFoundException notFound) {
          log.error("Cannot unsubscribe {} from {}: not found", name, fullQualifiedTopicName);
        }
      }
      if (!somethingDone) {
        // required for TCK, scan for all subscriptions
        List<String> allTopics = pulsarAdmin.topics().getList(systemNamespace);
        for (String topic : allTopics) {
          if (topic.endsWith(PENDING_ACK_STORE_SUFFIX)) {
            // skip Transaction related system topics
            log.info("Ignoring system topic {}", topic);
            continue;
          }
          log.info("Scanning topic {}", topic);
          List<String> subscriptions;
          try {
            subscriptions = pulsarAdmin.topics().getSubscriptions(topic);
            log.info("Subscriptions {}", subscriptions);
          } catch (PulsarAdminException.NotFoundException notFound) {
            log.error("Skipping topic {}", topic);
            subscriptions = Collections.emptyList();
          }
          for (String subscription : subscriptions) {
            log.info("Found subscription {} ", subscription);
            if (subscription.equals(name)) {
              log.info("deleteSubscription topic {} name {}", topic, name);
              pulsarAdmin.topics().deleteSubscription(topic, name, true);
              somethingDone = true;
            }
          }
        }
      }
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

  public synchronized boolean isForceDeleteTemporaryDestinations() {
    return forceDeleteTemporaryDestinations;
  }

  public String getQueueSubscriptionName(PulsarDestination destination) {
    String customSubscriptionName = destination.extractSubscriptionName();
    if (customSubscriptionName != null) {
      return customSubscriptionName;
    }
    synchronized (this) {
      return queueSubscriptionName;
    }
  }

  public synchronized long getWaitForServerStartupTimeout() {
    return waitForServerStartupTimeout;
  }

  public synchronized SubscriptionType getExclusiveSubscriptionTypeForSimpleConsumers(
      Destination destination) {
    return useExclusiveSubscriptionsForSimpleConsumers
        ? SubscriptionType.Exclusive
        : destination instanceof Queue ? SubscriptionType.Shared : getTopicSharedSubscriptionType();
  }

  public synchronized SubscriptionType getTopicSharedSubscriptionType() {
    return topicSharedSubscriptionType;
  }

  public String applySystemNamespace(String destination) {
    if (destination == null) {
      return null;
    }
    if (destination.startsWith("persistent://") || destination.startsWith("non-persistent://")) {
      return destination;
    }
    return "persistent://" + getSystemNamespace() + "/" + destination;
  }

  public boolean isAcknowledgeRejectedMessages() {
    return acknowledgeRejectedMessages;
  }

  public synchronized boolean isClosed() {
    return closed;
  }

  private synchronized int getPrecreateQueueSubscriptionConsumerQueueSize() {
    return precreateQueueSubscriptionConsumerQueueSize;
  }

  private synchronized void writeObject(ObjectOutputStream out) throws IOException {
    String serialisedConfiguration = new ObjectMapper().writeValueAsString(configuration);
    if (log.isDebugEnabled()) {
      log.debug("Serializing this PulsarConnectionFactory as {}", serialisedConfiguration);
    }
    out.writeUTF(serialisedConfiguration);
  }

  // this method must not be synchronizes, see RS_READOBJECT_SYNC
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    resetDefaultValues();
    String readConfiguration = in.readUTF();
    if (log.isDebugEnabled()) {
      log.debug("Deserialize configuration as {}", configuration);
    }
    try {
      setJsonConfiguration(readConfiguration);
    } catch (Exception err) {
      throw new IOException("Cannot decode JSON configuration " + configuration);
    }
  }

  private void setFinalField(String name, Object value) {
    try {
      Field field = this.getClass().getDeclaredField(name);
      boolean accessible = field.isAccessible();
      if (!accessible) {
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
      }
      try {
        field.set(this, value);
      } finally {
        if (!accessible) {
          field.setAccessible(false);
          Field modifiersField = Field.class.getDeclaredField("modifiers");
          modifiersField.setAccessible(true);
          modifiersField.setInt(field, field.getModifiers() | Modifier.FINAL);
        }
      }
    } catch (Exception err) {
      log.error("Error while setting final field {}", name, err);
      throw new RuntimeException(err);
    }
  }

  private synchronized void resetDefaultValues() {
    if (initialized) {
      throw new java.lang.IllegalStateException();
    }

    // final fields
    setFinalField("producers", new ConcurrentHashMap<>());
    setFinalField("connections", Collections.synchronizedSet(new HashSet<>()));
    setFinalField("consumers", new CopyOnWriteArrayList<>());
    setFinalField("readers", new CopyOnWriteArrayList<>());

    this.initialized = false;
    this.closed = false;
  }
}
