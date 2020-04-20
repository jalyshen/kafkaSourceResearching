消息Partition的选择过程
=======================

Kafka消息会被发送到哪个partition，有2种方式：
  * 消息自己确定需要发送到哪个Partition
  * Producer会计算出Partition ID，分配给当前的消息

一般而言，第二种方式是默认的方式，对于新消息总是通过Producer来分配Partition ID。
对于发送失败的消息而言，已经存在了Partition ID，因此就不再重复计算。

代码逻辑如下（KafkaProducer.java）：
```java
    private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
        Integer partition = record.partition();
        return partition != null ?
                partition :
                partitioner.partition(
                        record.topic(), record.key(), serializedKey, record.value(), serializedValue, cluster);
    }
```

来看看Partitioner如何计算一个消息的Partition ID的。

Partitioner.java对partition的定义如下：
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
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster);
```

Kafka提供了3种partition策略：
  * DefaultPartitioner
    <p>
    Kafka使用默认的Partitioner的几种情况：

    * 如果消息中已经指定了Partition ID，就使用这个Partitioner

    * 消息中没有指定Partition ID，但是消息中的Key使用Hash方式选择了一个Partition Id，也使用这个Partitioner

    * 如果没有预先设定Partition ID，或者Key也没有选择StickyPartitioner，
    </p>

  * RoundRobinPartitioner
    <p>
    
    </p>

  * UniformStickyPartitioner

