Kafka消费端消息消息源码分析
======================================================

# 使用Kafka Consumer消费消息的方式

下面一段代码，展示了Kafka Consumer API的极简使用方式：使用了“Automatic Offset Committing”的方式获取消息。

```java
 
  //===================================
  // 创建一个KafkaConsumer对象
  //   1. 设置相应的的属性（此处省略）
  //   2. 创建一个kafkaConsumer的实例
  //===================================
  Properties props = new Properties();
  props.setProperty("bootstrap.servers", "localhost:9092");
  props.setProperty("group.id", "test");
  props.setProperty("enable.auto.commit", "true");
  props.setProperty("auto.commit.interval.ms", "1000");
  props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
  //==============================
  // 订阅名为“foo”和“bar”的Topic
  //==============================
  consumer.subscribe(Arrays.asList("foo", "bar"));
  while (true) {
      //==============================
      //  从服务器开始拉取数据，开始消费
      //==============================
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records)
        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
  }
```

还可以通过手动控制Offset的方式获取消息：
```java
    //===================================
    // 创建一个KafkaConsumer对象
    //   1. 设置相应的的属性（此处省略）
    //   2. 创建一个kafkaConsumer的实例
    //===================================
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("group.id", "test");
    props.setProperty("enable.auto.commit", "false");
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    //==============================
    // 订阅名为“foo”和“bar”的Topic
    //==============================
    consumer.subscribe(Arrays.asList("foo", "bar"));
    // 设置处理消息缓冲的大小
    final int minBatchSize = 200;
    List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
    while (true) {
        //==============================
        //  从服务器开始拉取数据，开始消费
        //==============================
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
            buffer.add(record);
        }
        // 手动清楚cache，同步获取消息的offset
        if (buffer.size() >= minBatchSize) {
            insertIntoDb(buffer);
            consumer.commitSync();
            buffer.clear();
        }
    }
```

具体的API使用还有很多方式，读者可以自行阅读相关文档。

从API角度看，Kafka已经屏蔽了很多细节，让用户使用起来非常简单。现在需要探究期间的复杂性。

# Kafka拉取消息的过程

## 创建KafkaConsumer对象实例的过程

在实例化一个KafkaConsumer对象时，就进行了非常复杂的操作：
```java

    private KafkaConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        try {
            GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config,
                    GroupRebalanceConfig.ProtocolType.CONSUMER);

            this.groupId = groupRebalanceConfig.groupId;
            this.clientId = buildClientId(config.getString(CommonClientConfigs.CLIENT_ID_CONFIG), groupRebalanceConfig);

            LogContext logContext;

            // If group.instance.id is set, we will append it to the log context.
            if (groupRebalanceConfig.groupInstanceId.isPresent()) {
                logContext = new LogContext("[Consumer instanceId=" + groupRebalanceConfig.groupInstanceId.get() +
                        ", clientId=" + clientId + ", groupId=" + groupId + "] ");
            } else {
                logContext = new LogContext("[Consumer clientId=" + clientId + ", groupId=" + groupId + "] ");
            }

            this.log = logContext.logger(getClass());
            boolean enableAutoCommit = config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
            if (groupId == null) { // overwrite in case of default group id where the config is not explicitly provided
                if (!config.originals().containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
                    enableAutoCommit = false;
                else if (enableAutoCommit)
                    throw new InvalidConfigurationException(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG + " cannot be set to true when default group id (null) is used.");
            } else if (groupId.isEmpty())
                log.warn("Support for using the empty group id by consumers is deprecated and will be removed in the next major release.");

            log.debug("Initializing the Kafka consumer");
            this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
            this.time = Time.SYSTEM;
            this.metrics = buildMetrics(config, time, clientId);
            this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);

            // load interceptors and make sure they get clientId
            Map<String, Object> userProvidedConfigs = config.originals();
            userProvidedConfigs.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
            List<ConsumerInterceptor<K, V>> interceptorList = (List) (new ConsumerConfig(userProvidedConfigs, false)).getConfiguredInstances(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    ConsumerInterceptor.class);
            this.interceptors = new ConsumerInterceptors<>(interceptorList);
            if (keyDeserializer == null) {
                this.keyDeserializer = config.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
                this.keyDeserializer.configure(config.originals(), true);
            } else {
                config.ignore(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
                this.keyDeserializer = keyDeserializer;
            }
            if (valueDeserializer == null) {
                this.valueDeserializer = config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
                this.valueDeserializer.configure(config.originals(), false);
            } else {
                config.ignore(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
                this.valueDeserializer = valueDeserializer;
            }
            OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.valueOf(config.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).toUpperCase(Locale.ROOT));
            this.subscriptions = new SubscriptionState(logContext, offsetResetStrategy);
            ClusterResourceListeners clusterResourceListeners = configureClusterResourceListeners(keyDeserializer,
                    valueDeserializer, metrics.reporters(), interceptorList);
            this.metadata = new ConsumerMetadata(retryBackoffMs,
                    config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG),
                    !config.getBoolean(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG),
                    config.getBoolean(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG),
                    subscriptions, logContext, clusterResourceListeners);
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                    config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), config.getString(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG));
            this.metadata.bootstrap(addresses);
            String metricGrpPrefix = "consumer";

            FetcherMetricsRegistry metricsRegistry = new FetcherMetricsRegistry(Collections.singleton(CLIENT_ID_METRIC_TAG), metricGrpPrefix);
            ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config, time);
            IsolationLevel isolationLevel = IsolationLevel.valueOf(
                    config.getString(ConsumerConfig.ISOLATION_LEVEL_CONFIG).toUpperCase(Locale.ROOT));
            Sensor throttleTimeSensor = Fetcher.throttleTimeSensor(metrics, metricsRegistry);
            int heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);

            ApiVersions apiVersions = new ApiVersions();
            NetworkClient netClient = new NetworkClient(
                    new Selector(config.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), metrics, time, metricGrpPrefix, channelBuilder, logContext),
                    this.metadata,
                    clientId,
                    100, // a fixed large enough value will suffice for max in-flight requests
                    config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                    config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                    config.getInt(ConsumerConfig.SEND_BUFFER_CONFIG),
                    config.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG),
                    config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                    ClientDnsLookup.forConfig(config.getString(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG)),
                    time,
                    true,
                    apiVersions,
                    throttleTimeSensor,
                    logContext);
            this.client = new ConsumerNetworkClient(
                    logContext,
                    netClient,
                    metadata,
                    time,
                    retryBackoffMs,
                    config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                    heartbeatIntervalMs); //Will avoid blocking an extended period of time to prevent heartbeat thread starvation

            this.assignors = getAssignorInstances(config.getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG), config.originals());

            // no coordinator will be constructed for the default (null) group id
            this.coordinator = groupId == null ? null :
                new ConsumerCoordinator(groupRebalanceConfig,
                        logContext,
                        this.client,
                        assignors,
                        this.metadata,
                        this.subscriptions,
                        metrics,
                        metricGrpPrefix,
                        this.time,
                        enableAutoCommit,
                        config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG),
                        this.interceptors);
            this.fetcher = new Fetcher<>(
                    logContext,
                    this.client,
                    config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
                    config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG),
                    config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG),
                    config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG),
                    config.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG),
                    config.getBoolean(ConsumerConfig.CHECK_CRCS_CONFIG),
                    config.getString(ConsumerConfig.CLIENT_RACK_CONFIG),
                    this.keyDeserializer,
                    this.valueDeserializer,
                    this.metadata,
                    this.subscriptions,
                    metrics,
                    metricsRegistry,
                    this.time,
                    this.retryBackoffMs,
                    this.requestTimeoutMs,
                    isolationLevel,
                    apiVersions);

            this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, metricGrpPrefix);

            config.logUnused();
            AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds());
            log.debug("Kafka consumer initialized");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed; this is to prevent resource leak. see KAFKA-2121
            close(0, true);
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka consumer", t);
        }
    }
    
```