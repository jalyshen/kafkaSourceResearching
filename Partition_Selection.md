消息Partition的选择过程
=======================

Kafka消息会被发送到哪个partition，有2种方式：
  * 消息自己确定需要发送到哪个Partition (*人为指定*)
  * Producer会计算出Partition ID，分配给当前的消息 （*系统会根据配置信息自动分配*）

一般而言，第二种方式是默认的方式，对于新消息总是通过Producer来分配Partition ID。
如果当前消息已经存在了Partition ID，就不再重复计算。

代码逻辑如下(KafkaProducer.java)：
```java
    private int partition(ProducerRecord<K, V> record, 
                          byte[] serializedKey, 
                          byte[] serializedValue, 
                          Cluster cluster) 
    {        
        Integer partition = record.partition();
        // 如果消息已经设置了Partition，就直接使用；否则 Kafka 开始根据相关信息分配
        return partition != null ? partition :
                                       partitioner.partition(
                                       record.topic(), record.key(), 
                                       serializedKey, record.value(), 
                                       serializedValue, cluster);
    }
```

来看看Partitioner如何计算一个消息的Partition ID的。

Partitioner.java对partition方法的定义如下：
```java
    /**
     * Compute the partition for the given record.
     *
     * @param topic      The topic name
     * @param key        The key to partition on (or null if no key)
     * @param keyBytes   The serialized key to partition on( or null if no key)
     * @param value      The value to partition on or null
     * @param valueBytes The serialized value to partition on or null
     * @param cluster    The current cluster metadata
     */
    public int partition(String topic, 
                         Object key, 
                         byte[] keyBytes, 
                         Object value, 
                         byte[] valueBytes, 
                         Cluster cluster);
```

Kafka提供了3种partition策略(实现)：
  * **DefaultPartitioner**: 默认的Partition计算方式
  * **RoundRobinPartitioner**: 轮询方式计算方法
  * **UniformStickyPartitioner**: 均匀分布的计算方式

## DefaultPartitioner

```java
  public int partition(String topic, 
                       Object key, 
                       byte[] keyBytes, 
                       Object value, 
                       byte[] valueBytes, 
                       Cluster cluster) 
  {
        // 如果没有Key，采用UniformSticky方式                         
        if (keyBytes == null) {
            return stickyPartitionCache.partition(topic, cluster);
        }
        //==========================================
        // 获取当前Topic一共有多少个Partitons
        //==========================================
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        // hash the keyBytes to choose a partition
        //================================================================
        // 根据消息的Key(Byte)计算murmur2的哈希值（理解为第二代murmur）。
        // murmur hash，是一种非加密型的一致性哈希
        // 这种Hash计算方法性能高，碰撞率低。
        // 然后计算出的 hash 值按照 分区的数量进行取模计算，结果就是对应的分区号
        //================================================================
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
  }
```

## RoundRobinPartitioner
```java
  public int partition(String topic, 
                       Object key, 
                       byte[] keyBytes, 
                       Object value, 
                       byte[] valueBytes, 
                       Cluster cluster) 
  {
        //==========================================
        // 获取当前Topic的Partitons数量
        //==========================================
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        // 这个值，表示当前追加的消息在指定的Topic是第几条
        int nextValue = nextValue(topic);

        //======================================
        // 获取当前Topic可用的Partitons数量。
        // 这个数量应该<=上一步计算的numPartitions
        //======================================
        List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);

        //===================================================
        // 如果有可用的partitons,就根据有效的partition数量求值
        // 否则，就根据Topic原有总数量求值
        //===================================================
        if (!availablePartitions.isEmpty()) {
            int part = Utils.toPositive(nextValue) % availablePartitions.size();
            return availablePartitions.get(part).partition();
        } else {
            // no partitions are available, give a non-available partition
            return Utils.toPositive(nextValue) % numPartitions;
        }
  }

  /**
   * 采用轮询方式，就需要partitioner自身需要维护一个表，
   * 用于Topic统计每个Topic已经追加的消息数量
   */
  private int nextValue(String topic) {
        //========================================
        // 如果当前的topic有数据了，就返回；
        // 否则，给指定的topic新建一个计数器，初始值为0
        //========================================
        AtomicInteger counter = topicCounterMap.computeIfAbsent(topic, k -> {
            return new AtomicInteger(0);
        });
        return counter.getAndIncrement();
  }
```

## UniformStickyPartitioner
```java

  UniformStickyPartitioner.java:

  public int partition(String topic, 
                       Object key, 
                       byte[] keyBytes, 
                       Object value, 
                       byte[] valueBytes, 
                       Cluster cluster) 
  {
    return stickyPartitionCache.partition(topic, cluster);
  }

  //---------------------------------------------------------------------------

  StickyPartitionCache.java:

  // 节点自身维护的一个Map
  private final ConcurrentMap<String, Integer> indexCache;

  // 重点计算在nextPartition()中： 
  public int partition(String topic, Cluster cluster) {
        Integer part = indexCache.get(topic);
        //==========================================
        // 如果是第一条消息(可能当前节点重启过)，就要计算
        //==========================================
        if (part == null) {
            return nextPartition(topic, cluster, -1);
        }
        return part;
  }

  
  public int nextPartition(String topic, Cluster cluster, int prevPartition) {
    //==========================================
    // 获取当前Topic的Partitons总数量
    //==========================================
    List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
    Integer oldPart = indexCache.get(topic);
    Integer newPart = oldPart;
    
    // Check that the current sticky partition for the topic 
    // is either not set or that the partition that 
    // triggered the new batch matches the sticky partition that needs to be changed.
    if (oldPart == null || oldPart == prevPartition) {
      List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
      if (availablePartitions.size() < 1) {
        Integer random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
        newPart = random % partitions.size();
      } else if (availablePartitions.size() == 1) {
        newPart = availablePartitions.get(0).partition();
      } else {
        while (newPart == null || newPart.equals(oldPart)) {
          Integer random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
          newPart = availablePartitions.get(random % availablePartitions.size()).partition();
        }
      }
      // Only change the sticky partition if it is null 
      // or prevPartition matches the current sticky partition.
      if (oldPart == null) {
        indexCache.putIfAbsent(topic, newPart);
      } else {
        indexCache.replace(topic, prevPartition, newPart);
      }
        return indexCache.get(topic);
    }
    
    return indexCache.get(topic);
  }

```
