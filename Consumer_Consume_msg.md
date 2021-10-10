Kafka消费端消息消息源码分析
======================================================

## 一. 使用Kafka Consumer消费消息的方式

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
  props.setProperty("key.deserializer", 
                    "org.apache.kafka.common.serialization.StringDeserializer");
  props.setProperty("value.deserializer", 
                    "org.apache.kafka.common.serialization.StringDeserializer");
  KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
  //==============================
  // 订阅名为“foo”和“bar”的Topic
  //==============================
  consumer.subscribe(Arrays.asList("foo", "bar"));
  while (true) {
      //==============================
      //  从服务器开始拉取数据，开始消费
      //  每隔 100ms 拉取一次数据
      //==============================
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records)
        System.out.printf("offset = %d, key = %s, value = %s%n", 
                          record.offset(), 
                          record.key(), 
                          record.value());
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
    props.setProperty("key.deserializer", 
                      "org.apache.kafka.common.serialization.StringDeserializer");
    props.setProperty("value.deserializer", 
                      "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    //==============================
    // 订阅名为“foo”和“bar”的Topic
    //==============================
    consumer.subscribe(Arrays.asList("foo", "bar"));
    // 设置处理消息缓冲的最小容量
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
        //=======================================
        // 如果取回的数据量超过了最小设定的缓存大小
        // 就把取回的数据存入 DB，
        // 然后手动提交 committed Offset 到 broker
        // 最后，手动清除 cache
        //========================================
        if (buffer.size() >= minBatchSize) {
            insertIntoDb(buffer);
            consumer.commitSync();
            buffer.clear();
        }
    }
```

​        从API角度看，Kafka已经屏蔽了很多细节，让用户使用起来非常简单。现在需要探究期间的复杂性。

## 二. Kafka拉取消息的过程

### 2.1 创建KafkaConsumer对象实例的过程

​        在实例化一个KafkaConsumer对象时，就进行了非常复杂的操作。
​        初始化过程只要完成的操作有：

​        //TODO: 添加初始化的主要内容

### 2.2 订阅Topic

​        subscribe()方法的主要作用，就是 check 一下订阅的Topic的可用性，并为订阅消息提供一个可用的存储空间（buffer），并更新订阅topic的列表。

​        使用subscribe方法时，通常只使用一个参数的方法。但是底层会默认传递一个 NoOpConsumerRebalanceListener  实例到下面的方法中：

```java
    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        acquireAndEnsureOpen();
        try {
            //=====================================
            // 确保group id是存在的。否则抛出异常
            //=====================================
            maybeThrowInvalidGroupIdException();
            //=====================================
            // 确保传入的topic是存在的
            //=====================================
            if (topics == null)
                throw new IllegalArgumentException(" Topic collection to subscribe " +
                                                   " to cannot be null");
            if (topics.isEmpty()) {
                // treat subscribing to empty topic list as the same as unsubscribing
                this.unsubscribe();
            } else {
                for (String topic : topics) {
                    if (topic == null || topic.trim().isEmpty())
                        throw new IllegalArgumentException(" Topic collection to subscribe " +
                                                           " to cannot contain null " +
                                                           " or empty topic");
                }

                //========================================
                // 确保初始化了“自定义分区”的分配，否则抛出异常
                //========================================
                throwIfNoAssignorsConfigured();

                //=============================================================
                // Fetcher是一个线程安全的类，主要管理从broker获取数据的过程。
                // 在订阅一组新的topic前，需要先清理出一些buffer空间分配给新订阅的topic
                //=============================================================
                fetcher.clearBufferedDataForUnassignedTopics(topics);                
                log.info("Subscribed to topic(s): {}", Utils.join(topics, ", "));

                //=============================================================
                // 1. subscriptions，是一个SubscriptionState对象的实例。
                //    这个类，主要职责是为消费组追踪topic、partition和offset信息的
                // 2. 如果Consumer设置了Rebalance的监听器，此刻就会把Consumer的监听器
                //    注册到系统里
                // 3. 同时，更新subscriptions订阅的主题信息
                //=============================================================
                if (this.subscriptions.subscribe(new HashSet<>(topics), listener))
                    metadata.requestUpdateForNewTopics();
            }
        } finally {
            release();
        }
    }
```

### 2.3 拉取消息

​        Consumer拉取消息比较简单，通常是这样：
```java
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
```
​        在一个设定的时间范围内拉取数据。通常，poll方法会立即返回数据。但是如果broker没有数据，会等待一段时间，直到超过设定的时间，然后返回一个空的记录集。

​        poll是一个多态的，继续内部调用的方法多了一个boolean参数（*includeMetadataInTimeout*），默认是true。

```java
    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        return poll(time.timer(timeout), true);
    }
```
​        这个includeMetadataInTimeout默认true，是Consumer拉取数据超时之后阻塞，以便执行自定义的ConsumerRebalanceListener回调。 
​        而这个ConsumerRebalanceListener是在subscribe()时设定的：

```java
    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    private ConsumerRecords<K, V> poll(final Timer timer, 
                                       final boolean includeMetadataInTimeout) {
        //============================================
        // 1. 看看当前的cousumer是不是已经有一个线程为其服务了，
        //    保证当前操作是线程安全的
        // 2. 检查当前的consumer是否已经关闭
        //============================================
        acquireAndEnsureOpen();
        
        try {
            this.kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs());

            if (this.subscriptions.hasNoSubscriptionOrUserAssignment()) {
                throw new IllegalStateException(" Consumer is not subscribed " +
                                                " to any topics or assigned any partitions");
            }

            //==================================================
            // poll for new data until the timeout expires
            //==================================================
            do {
                //===========================================
                // 确定网络是否已经在工作状态。
                // client，是ConsumerNetworkClient的一个实例，
                // 是Consumer访问网络层的一个高层抽象对象
                //===========================================
                client.maybeTriggerWakeup();

                //===========================================================================
                // 这里的 updateAssignmentMetadataIfNeeded() 
                //       方法涉及到一个ConsumerCoordinator对象，
                // 这个coordinate对象在这个方法里也进行poll(timer)操作。
                // 主要完成的任务是轮询coodinator的事件。 目的是：
                //    1. 确保当前的cosumer所拥有的coordinate是能被broker;
                //    2. 确保当前的cousumer已经在某个Group中
                //    3. 周期性的处理offset的提交
                //===========================================================================
                if (includeMetadataInTimeout) {
                    if (!updateAssignmentMetadataIfNeeded(timer)) {
                        return ConsumerRecords.empty();
                    }
                } else {
                    while (!updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))) {
                        log.warn("Still waiting for metadata");
                    }
                }

                final Map<TopicPartition, List<ConsumerRecord<K, V>>> 
                            records = pollForFetches(timer);
                if (!records.isEmpty()) {
                    // before returning the fetched records, we can send off 
                    // the next round of fetches and avoid block waiting for
                    // their responses to enable pipelining while the user
                    // is handling the fetched records.
                    //
                    // NOTE: since the consumed position has already been updated, 
                    // we must not allow wakeups or any other errors
                    // to be triggered prior to returning the fetched records.
                    if (fetcher.sendFetches() > 0 || client.hasPendingRequests()) {
                        client.transmitSends();
                    }
                    return this.interceptors.onConsume(new ConsumerRecords<>(records));
                }
            } while (timer.notExpired());
            return ConsumerRecords.empty();
        } finally {
            release();
            this.kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs());
        }
    }
```