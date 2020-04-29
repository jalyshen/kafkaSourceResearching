# 深入Kafka内部 (Kafka Inside)

通过分析Kafka的源码，深入了解Kafka

<b>如果转载，请注明出处！</b>

目前Kafka最新release的代码版本是2.4.1，此次源码分析基于此版本。 
按照Kafka官方的核心模块进行源码学习。

* Java客户端源码分析
  * 消息生产端源码分析
    * [Kafka生产者发送消息的过程](KafkaProducer_send_msg.md)
    * [Kafka为消息选择Partition的过程](Partition_Selection.md)
  * 消息消费端
    * [消息消费设计思路介绍](Consumer_design.md)
    * [消息消费端源码分析](Consumer_Consume_msg.md)
  * 通用组件源码分析
* 服务器端源码分析
  * 日志模块
    * [Kafka消息落盘的原理分析](KafkaServer_Persist_Message_theory.md)
  * 请求处理模块
  * Controller模块
  * 状态机模块
  * 延迟操作模块
  * 副本管理模块
  * 消费者组管理模块
* Streams组件源码分析
  * kstream模块
  * processor模块
  * state模块
  * 内部组件模块
* Connect组件源码分析
  * 运行时模块
  * API模块
  * Connector模块
* 附录
  * [NIO介绍](nio_knowledge.md)

