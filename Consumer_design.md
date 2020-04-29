消息消费设计思路介绍
======================================================

- [消费端的重要概念](#消费端的重要概念)
  - [Consumer Group](#Consumer Group)
  - [Consume Position](#Consume Position)
- [Offset管理](#Offset管理)
  - [设置offset的提交方式](#设置offset的提交方式)
  - [消费端管理Offset](#消费端管理Offset)
  - [Broker管理Offset](#Broker管理Offset)


分析源码前，先了解一下Kafka Consumer这个Clinet的设计思路。参考的Kafka版本是2.4.1

已有大神把这个讲得很清晰了。这里引用一下[《Kafka消费组(consumer group)》](https://www.cnblogs.com/huxi2b/p/6223228.html)。

# 消费端的重要概念

## Consumer Group

Kafka的Group，是一种机制，用来实现<b>扩展性</b>和<b>容错性</b>。

Consumer Group的特性大致如下：
  * 一个Group可以拥有一个或多个Consumer，他们共享一个Group ID。
  * 一个Group内的所有Consumer相互合作，共同消费订阅的Topic（可能是一组，多个Topic）的<b>所有分区(Partition)</b>。
  * 每个分区的消息，<b>只能</b>被Group里的<font color=red><b>一个</b></font>Consumer消费。

## Consume Position

通俗的讲，Consume Position表示一个Consumer已经消费了某个Topic-Partition的多少消息。Consume Position相当与一个指针，指向已经消费到topic-partition消息队列的某个消息的位置。Kafka针对这个位置信息提供了专门的术语：offset。

以下文档，使用术语"offset"。

从<b>语义</b>上来区分，分为以下两种offset：
  * <b>Current Offset</b>：保存在Consumer Group(client端)，表示Consumer希望收到的下一条信息的序号。当且仅当consumer去poll()消息时使用
  * <b>Committed Offset</b>：保存在Kafka Broker(server端)，由Consumer定期发送给Broker，表示Consumer已经<font color=red><b>确认</b></font>消费过的消息序号。通过consumer的<font color=red><b>commitSync()</b></font>和<font color=red><b>commitAsync()</b></font>这2个API来完成

显然，Committed Offset会滞后于Current Offset。

Offset的根本作用，就是避免重复消费。使用方式不同：
  * <b>Current Offset</b>：
    * 表示Consumer下次消费消息的起点，每次poll都返回不重复的消息
    * 如果Consumer重启，可以继续从current offset的位置开始消费消息，避免造成重复消费
  * <b>Committed Offset</b>：
    * 如果Consumer Group进行了Re-balance，Topic-Partition会被分配给新的consumer。新consumer就从committed Offset位置开始消费消息，避免重复消费


# Offset管理

这里谈及的Offset管理，是面向Consumer + Broker的。

接下来分别介绍Offset在Consumer和Broker处理Offset的方法。

## 设置offset的提交方式

Kafka提供了2种方式，让Consumer提交Position给Broker：
  * 默认采用<font color=red><b>定期自动</b></font>提交position
  * Kafka也为用户提供了<b>手动模式</b> (配置项：enable.auto.commit = false)

Kafka的Cousumer消费了消息后，并不会马上把offset信息commit到Broker，而是定期发送。通过<font color=red><b>commitSync()</b></font>和<font color=red><b>commitAsync()</b></font>这2个API来完成offset的同步。

所以再次强调：<b>Broker上的Committed Offset数据可能会滞后于Consumer上的Current Offset</b>。

当前版本（2.4.1），Broker提供了一个特殊的Topic来保存Offset信息。这个特殊的Topic被命名为<b>_consumer_offsets</b>。

## 消费端管理Offset

消费端管理Offset，可以分为以下部分：
  * Current Offset
  * Group中共享Offset
  * 与Broker同步Offset

Consumer对象（或者具体的实例）中，存储一个SubscriptionState对象。一个Consumer可能会订阅多个Topic的消息，因此，通过一个PartitionStates<TopicPartitionState>来存储不同Topic消费的Offset。<b>TopicPartitionState</b>是真正存储offset的地方，具体通过FetchPosition对象来hold住offset信息。

这个关系如下：

```
KafkaConsumer
   |-- SubScriptionState
          |-- PartitionStates<TopicPartitionState>
                  |-- Map<TopicPartition, TopicPartitionState>
                                              |-- FetchPosition
```


## Broker管理Offset

