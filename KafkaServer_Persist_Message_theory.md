Kafka持久化消息
=================
- [探索Kafka消息的存储关系](#探索Kafak消息的存储关系)
- [存储消息](#存储消息)
  - [org.apache.kafka.common.record.FileRecords](#orgapachekafkacommonrecordfilerecords)
  - [JDK中的Channel](#jdk中的channel)
- [存储消息索引](#存储消息索引)

---

Kafka Server ( Borker)存储消息，不仅仅是消息本身，另一个重要的是消息的索引文件。接下来会针对这两种数据的持久化分别做说明。

## 一. 探索Kafak消息的存储关系
Kafka提供了很多概念，用于存储具体的消息。大家熟知的有：

  - Topic
  - Partition
  - Log
  - LogSegment
  - MessageSet (aka Records, there are several implements)
  - Message (aka Record)

官方也提供了对 Topic 这个概念的一个高度抽象的示意图: 

![](img/log_anatomy.png)

根据源码，把逻辑上的概念与真实的物理文件关系上了。下图展示了逻辑概念与具体文件的关系:

![](img/concepts_with_real_files.png)

具体的 File，就与 MessageSet（aka Records，具体实现就是FileRecoreds）紧密关联。所有的消息，都会<b>追加</b>到对应的 File 上。

Kafka Server 通过对象“KafkaServer.scala"来启动，启动时，会开启SocketServer，等待各个KafkaProducer来连接。同时，针对不同的请求，初始化各类的KafkaRequestsHandler，来处理不同的请求。关注对消息生产者消息的处理：

KafkaApis.scala：
```java
   def handleProduceRequest(request: RequestChannel.Request): Unit = {
       val produceRequest = request.body[ProduceRequest]
       ...
       //======================================
       // 把收到的消息，根据TopicPartition进行分类
       // 然后存储到这个容器里
       // mutable， Scala的一个可变容量的map容器
       //======================================
       val authorizedRequestInfo = mutable.Map[TopicPartition, MemoryRecords]()
       ...

       //===============================================
       // 根据不同的TopicPartition，对消息进行归类
       // 存储到刚刚定义的局部容器 authorizedRequestInfo 中
       //===============================================
       for ((topicPartition, memoryRecords) 
                      <- produceRequest.partitionRecordsOrFail.asScala) {
            if (!authorizedTopics.contains(topicPartition.topic))
                unauthorizedTopicResponses += 
                   topicPartition -> new PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED)
            else if (!metadataCache.contains(topicPartition))
                nonExistingTopicResponses += 
                    topicPartition -> new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION)
            else
                try {
                    ProduceRequest.validateRecords(request.header.apiVersion(), memoryRecords)
                    //==============================
                    // 根据TopicPartition，归类消息
                    //==============================
                    authorizedRequestInfo += (topicPartition -> memoryRecords)
                } catch {
                    case e: ApiException =>
                        invalidRequestResponses += 
                             topicPartition -> new PartitionResponse(Errors.forException(e))
                }
        }
        
        // 省略一堆代码
        ...

        if (authorizedRequestInfo.isEmpty)
            sendResponseCallback(Map.empty)
        else {
            val internalTopicsAllowed = request.header.clientId == AdminUtils.AdminClientId

            // call the replica manager to append messages to the replicas
            replicaManager.appendRecords(
                timeout = produceRequest.timeout.toLong,
                requiredAcks = produceRequest.acks,
                internalTopicsAllowed = internalTopicsAllowed,
                origin = AppendOrigin.Client,
                // 把消息追加到各个Partition中去
                entriesPerPartition = authorizedRequestInfo,
                responseCallback = sendResponseCallback,
                recordConversionStatsCallback = processingStatsCallback)

                // if the request is put into the purgatory, 
                // it will have a held reference and hence cannot be garbage collected;
                // hence we clear its data here in order to let GC reclaim its memory 
                // since it is already appended to log
            produceRequest.clearPartitionRecords()
        }
   }
```

现在看看 ReplicaManager.scala ：
```java
    def appendRecords(timeout: Long,
                    requiredAcks: Short,
                    internalTopicsAllowed: Boolean,
                    origin: AppendOrigin,
                    entriesPerPartition: Map[TopicPartition, MemoryRecords],
                    responseCallback: Map[TopicPartition, PartitionResponse] => Unit,
                    delayedProduceLock: Option[Lock] = None,
                    recordConversionStatsCallback: Map[TopicPartition, RecordConversionStats] 
                              => Unit = _ => ()): Unit = {
        if (isValidRequiredAcks(requiredAcks)) {
        val sTime = time.milliseconds
        //==========================
        // 追加消息到当前节点
        //==========================
        val localProduceResults = 
            appendToLocalLog( internalTopicsAllowed = internalTopicsAllowed,
                    origin, entriesPerPartition, requiredAcks)
        debug("Produce to local log in %d ms".format(time.milliseconds - sTime))
        ...

    }

    ...

    private def appendToLocalLog(internalTopicsAllowed: Boolean,
                               origin: AppendOrigin,
                               entriesPerPartition: Map[TopicPartition, MemoryRecords],
                               requiredAcks: Short): Map[TopicPartition, LogAppendResult] = {
        ...
        // 确定当前的消息追加到哪个Partition，是Lead的Partition
        val partition = getPartitionOrException(topicPartition, expectLeader = true)
        // 追加消息，返回LogAppendInfo对象。这里含有消息的offset信息
        val info = partition.appendRecordsToLeader(records, origin, requiredAcks)
        ...
    }
```

然后，调用 Log.scala ：
```java
    private def append(records: MemoryRecords,
                     origin: AppendOrigin,
                     interBrokerProtocolVersion: ApiVersion,
                     assignOffsets: Boolean,
                     leaderEpoch: Int): LogAppendInfo = {
        maybeHandleIOException(s"Error while appending records " + 
                               " to $topicPartition in dir ${dir.getParent}") {

        //====================================================================
        // 这里会计算当前的消息大小。 
        //----------------------------------------------
        // 方法里，判断大小（配置文件项： max.message.bytes，默认值1000012）：
        // if (batchSize > config.maxMessageSize) {
        //     brokerTopicStats.topicStats(topicPartition.topic)
        //                     .bytesRejectedRate.mark(records.sizeInBytes)
        //     brokerTopicStats.allTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
        //     throw new RecordTooLargeException(s"The record batch size in the " +
        //                      " append to $topicPartition is $batchSize bytes " +
        //          " which exceeds the maximum configured value of " +
        //          " ${config.maxMessageSize}.")
        // }
        //=========================================================================
        val appendInfo = analyzeAndValidateRecords(records, origin)

        // return if we have no valid messages or if this is 
        //    a duplicate of the last appended entry
        if (appendInfo.shallowCount == 0)
                return appendInfo

        //============================================================
        // 这里可能会出现消息丢失问题。
        // 如果消息的总长度超过了max.message.bytes的大小，就会被截断！！！！
        //============================================================
        // trim any invalid bytes or partial messages before appending it to the on-disk log
        var validRecords = trimInvalidBytes(records, appendInfo)
        ...

        //============================================================
        // 通过一些列的操作，把消息分配给某个Segment（LogSegment）执行追加工作
        // 因为一个Segment关联上一个具体的文件
        //============================================================
        segment.append(largestOffset = appendInfo.lastOffset,
          largestTimestamp = appendInfo.maxTimestamp,
          shallowOffsetOfMaxTimestamp = appendInfo.offsetOfMaxTimestamp,
          records = validRecords)
    }
```
接下里就是<b>LogSegment</b>了。因为Segment对应的就是真实的文件：
```java
    def append(largestOffset: Long,
             largestTimestamp: Long,
             shallowOffsetOfMaxTimestamp: Long,
             records: MemoryRecords): Unit = {
        ...
        //==================================================================
        // append the messages
        // 这个log实例，FileRecords （The file records containing log entries）
        // 这里的append方法，就是下面要详细介绍的地方了
        //==================================================================
        val appendedBytes = log.append(records)
        ...
    }
```

下面，就来分析这些FileRecordds是如何追加到File中的，速度是如此之快！

## 二. 存储消息

Kafka对消息的封装是对象:

​                *org.apache.kafka.common.record.Record (默认实现是: DefaultRecord)。*

但是，Kafka Server 不会单独记录一条消息到介质上，而是会汇集了一批记录后再持久化到存储介质上。Kafka Server 通过如下对 ***FileRecords*** 象来持久化消息的：

### 2.1 org.apache.kafka.common.record.FileRecords
这个类的官方解释：

​        *A Records implementation backed by a file. An optional start and end position can be applied to this instance to enable slicing a range of the log records.*

现在，只关注此对象的“写”操作。通过阅读官方文档，Kafka持久化消息时，采用的是**顺序读写**的方式，所有的数据是 “**追加**” 到现有文件（Partition）的后面。因此，核心方法也是这里的“append”方法：

```java
  public class FileRecords extends AbstractRecords implements Closeable {    
    private final boolean isSlice;
    private final int start;
    private final int end;

    // 
    private final Iterable<FileLogInputStream.FileChannelRecordBatch> batches;

    // mutable state
    private final AtomicInteger size;

    // 把Message写入到文件的通道。这个是核心对象
    private final FileChannel channel;

    // 文件句柄
    private volatile File file;

    //此处省略n行代码    
    ..............

    //=================================
    // 核心代码
    // 这里开始，承接上面介绍的append方法了
    //=================================
    /**
     * Append a set of records to the file. This method is not thread-safe and must be
     * protected with a lock.
     * 这个方法，就是把记录（Message）写到文件里。
     * 注意：这个方法不是线程安全的，需要加锁。
     *
     * @param records The records to append
     * @return the number of bytes written to the underlying file
     */
    public int append(MemoryRecords records) throws IOException {
        // 先判断要写入的信息，是否超出了一个 segment 的大小
        if (records.sizeInBytes() > Integer.MAX_VALUE - size.get())
            throw new IllegalArgumentException(
                      "Append of size " + records.sizeInBytes() 
                    + " bytes is too large for segment with current file position at " 
                    + size.get());
        //============================================
        //最为关键的一行，把数据写到channel (FileChannel)
        //FileChannel 会负责把数据持久化到文件中
        //============================================
        int written = records.writeFullyTo(channel);

        size.getAndAdd(written); // Atomic类型的CAS操作，更新大小
        return written;
    }

    /**
     * Commit all written data to the physical disk
     */
    public void flush() throws IOException {
        channel.force(true);
    }

    /**    
     * 接口中的描述：试图把缓存中的内容写到一个通道中。
     *
     * 这个方法，用于把数据发送其他节点、消费者。 暂时可以忽略
     */
    @Override
    public long writeTo(GatheringByteChannel destChannel, long offset, int length) 
                throws IOException {
        long newSize = Math.min(channel.size(), end) - start;
        int oldSize = sizeInBytes();
        if (newSize < oldSize)
            throw new KafkaException(String.format(
                    "Size of FileRecords %s has been truncated during " +
                         " write: old size %d, new size %d ",
                         file.getAbsolutePath(), oldSize, newSize));

        long position = start + offset;
        int count = Math.min(length, oldSize);
        final long bytesTransferred;
        if (destChannel instanceof TransportLayer) {
            TransportLayer tl = (TransportLayer) destChannel;
            bytesTransferred = tl.transferFrom(channel, position, count);
        } else {
            //============================================================
            // 调用NIO，零拷贝方式：磁盘 -> 内核空间  - >目的缓冲区
            //============================================================
            bytesTransferred = channel.transferTo(position, count, destChannel);
        }
        return bytesTransferred;
    }
  }
```

现在来看看 *writeFullyTo()* 是如何做的。 

这个方法归属于 MemoryRecords(*MemoryRecords.java*) 对象:

```java    
    public int writeFullyTo(GatheringByteChannel channel) throws IOException {
        buffer.mark();
        int written = 0;
        while (written < sizeInBytes())
            //================================================
            // 这里最核心了。涉及到了OS（研究对象是Linux）的内核态了
            //================================================
            written += channel.write(buffer);

        //============================================
        // 写完数据后，把buffer的position指针指向上次的位置
        // 为了复用此buffer
        //============================================
        buffer.reset();
        return written;
    }
```

这个时候，就要深入的了解一下Channel如何实现write(ByteBuffer src)。

通常，用户态的程序的某一个操作需要内核态来协助完成(*例如读取磁盘上的一段数据*)，那么用户态的程序就会通过系统调用来调用内核态的接口，请求操作系统来完成某种操作。<b>此时，用户空间的数据，需要COPY一份到内核空间。</b>换句话说，所有的I/O操作，都是在内核态完成。

### 2.2 JDK中的Channel

MemoryRecords使用的channle是接口<b>GatheringByteChannel</b>。官方的说明是：

    A channel that can write bytes from a sequence of buffers.

这里研究的是FileChannelImpl这个实现类:
```java
    public int write(ByteBuffer src) throws IOException {
        ensureOpen();
        if (!writable)
            throw new NonWritableChannelException();
        synchronized (positionLock) {
            if (direct)
                //========================================================
                // alignment: IO alignment value for DirectIO
                // 对于Linux 2.4.10+ ，BlockSize大小： 4096(Byte) = 4KB
                // 需要与Page Cache页面对齐：
                // 用于传递数据的缓冲区，其内存边界必须对齐为 BlockSize 的整数倍
                // 用于传递数据的缓冲区，其传递数据的大小必须是 BlockSize 的整数倍。
                // 数据传输的开始点，即文件和设备的偏移量，必须是 BlockSize 的整数倍
                // 
                // 回顾一下： Producer中，设定batch.size的默认值是16384(Byte)，
                // 16384 % 4096 = 4
                // 而Linux的Page Cache正好是由4个Block组成
                //=========================================================
                Util.checkChannelPositionAligned(position(), alignment);
            int n = 0;
            int ti = -1;
            try {
                beginBlocking();
                ti = threads.add();
                if (!isOpen())
                    return 0;
                do {
                    //==========================================================
                    // 数据写入
                    // fd: FileDescriptor 是文件描述符，用来表示开放文件、开放套接字等
                    //     相当于文件句柄
                    // nd: FileDispatcher 用于不同的平台调用native()方法来完成read和write操作。
                    //==========================================================
                    n = IOUtil.write(fd, src, -1, direct, alignment, nd);
                } while ((n == IOStatus.INTERRUPTED) && isOpen());
                return IOStatus.normalize(n);
            } finally {
                threads.remove(ti);
                endBlocking(n > 0);
                assert IOStatus.check(n);
            }
        }
    }
```

IOUtil.java 中的 write():
```java
    static int write(FileDescriptor fd, ByteBuffer src, long position,
                     boolean directIO, int alignment, NativeDispatcher nd)
        throws IOException
    {
        //==========================================
        // 是否使用的是DirectBuffer
        // 如果是，则直接写
        // 否则，Kafka帮忙对齐Page Cache，然后再写
        //==========================================
        if (src instanceof DirectBuffer) {
            return writeFromNativeBuffer(fd, src, position, directIO, alignment, nd);
        }

        // Substitute a native buffer
        int pos = src.position();
        int lim = src.limit();
        assert (pos <= lim);
        int rem = (pos <= lim ? lim - pos : 0);
        ByteBuffer bb;
        //==========================================
        // 这里，Kafka帮忙对齐Page Cache，
        // 而且，都是返回DirectBuffer的实例
        //==========================================
        if (directIO) {
            Util.checkRemainingBufferSizeAligned(rem, alignment);
            bb = Util.getTemporaryAlignedDirectBuffer(rem, alignment);
        } else {
            bb = Util.getTemporaryDirectBuffer(rem);
        }
        try {
            bb.put(src);
            bb.flip();
            // Do not update src until we see how many bytes were written
            src.position(pos);

            int n = writeFromNativeBuffer(fd, bb, position, directIO, alignment, nd);
            if (n > 0) {
                // now update src
                src.position(pos + n);
            }
            return n;
        } finally {
            Util.offerFirstTemporaryDirectBuffer(bb);
        }
    }
```

IOUtil.java 中的 writeFromNativeBuffer():
```java
    private static int writeFromNativeBuffer(FileDescriptor fd, ByteBuffer bb,
                                             long position, boolean directIO,
                                             int alignment, NativeDispatcher nd)
        throws IOException
    {
        int pos = bb.position();
        int lim = bb.limit();
        assert (pos <= lim);
        int rem = (pos <= lim ? lim - pos : 0);

        if (directIO) {
            Util.checkBufferPositionAligned(bb, pos, alignment);
            Util.checkRemainingBufferSizeAligned(rem, alignment);
        }

        int written = 0;
        if (rem == 0)
            return 0;
        //==============================================
        // 真正执行“写”的地方。
        // 使用nd的pwrite()方法。此时，数据真正的算是“落盘”了
        //==============================================
        if (position != -1) {
            written = nd.pwrite(fd,
                                ((DirectBuffer)bb).address() + pos,
                                rem, position);
        } else {
            written = nd.write(fd, ((DirectBuffer)bb).address() + pos, rem);
        }
        if (written > 0)
            bb.position(pos + written);
        return written;
    }
```

上面代码的nd（NativeDispatcher），使用的实现类便是FileDispatcherImpl.java，其pwrite():
``` java

    // 就是简单的调用了native方法
    int pwrite(FileDescriptor fd, long address, int len, long position)
        throws IOException
    {
        return pwrite0(fd, address, len, position);
    }

    // 这就是native方法
    static native int pwrite0(FileDescriptor fd, long address, int len,
                             long position) throws IOException;


```

此时，从JDK的Java源码已经到头了，是到了看看FileDispatcherImpl的Native实现的时候了。 这里找来了OpenJDK的源码。通过OpenJDK源码来看看最终是如何调用OS来完成“落盘”的。

源码链接：[FileDispatcherImpl.c](https://github.com/openjdk/jdk/blob/1691abc7478bb83bd213b325007f14da4d038651/src/java.base/unix/native/libnio/ch/FileDispatcherImpl.c)

``` c
    #define pwrite64 pwrite  # 感兴趣的，可以阅读Linux的源码 pwrite() 方法

    JNIEXPORT jint JNICALL
     Java_sun_nio_ch_FileDispatcherImpl_pwrite0(JNIEnv *env, jclass clazz, jobject fdo,
                            jlong address, jint len, jlong offset)
    {
        jint fd = fdval(env, fdo);
        void *buf = (void *)jlong_to_ptr(address);

        return convertReturnVal(env, pwrite64(fd, buf, len, offset), JNI_FALSE);
    }

```
到这里，针对Kafka Server如何持久化消息的全部过程就了解了。

那么，对于消费者，Kafak Server有义务为它们提供快速检索消息的服务。那么，如何能快速地定位一条消息呢？Kafka Server为每个消息建立了相应的索引，并针对索引文件的存储提供了特殊的方法。接下来看看Kafka是如何做的呢？

## 三. 存储消息索引
索引，就是为了快速查找到相应的消息记录，因此，索引文件需要常驻内存，并能及时的更新索引的数据，因为随时都有新的消息追加到服务器中。同时，这个索引文件需要随时存储到介质上，以防丢失。例如，服务器宕机后，Kafka Server能够重新装载索引文件，继续服务。

如何保证索引文件能够及时的被更新，并及时的持久化到存储介质上呢？Kafka又一次利用了JDK7后提供的新特性：**mmap**

关于mmap的介绍，请参考[NIO](nio_knowledge.md)介绍。现在来看看Kafka的索引是如何创建和映射的：

### 3.1 消息的索引文件创建

```scala
  @volatile
  protected var mmap: MappedByteBuffer = {
    // 创建一个文件，用于存储消息的索引  
    val newlyCreated = file.createNewFile()
    val raf = if (writable) new RandomAccessFile(file, "rw") else new RandomAccessFile(file, "r")
    try {
      /* pre-allocate the file if necessary */
      if(newlyCreated) {
        if(maxIndexSize < entrySize)
          throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize)
        raf.setLength(roundDownToExactMultiple(maxIndexSize, entrySize))
      }
    
      //=================================
      // 核心代码 - 把文件映射到内存
      /* memory-map the file */
      //=================================
      _length = raf.length()
      val idx = {
        if (writable)
          raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, _length)
        else
          raf.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, _length)
      }
      //=======================================================
      // 如果是新建的索引，设置头指针位置
      /* set the position in the index for the next entry */
      //=======================================================
      if(newlyCreated)
        idx.position(0)
      else
        //==============================================================
        // 如果是已经存在的索引文件，获取最有一个日志实体的位置作为当前的索引位置
        // if this is a pre-existing index, 
        // assume it is valid and set position to last entry
        //==============================================================
        idx.position(roundDownToExactMultiple(idx.limit(), entrySize))
      idx
    } finally {
      CoreUtils.swallow(raf.close(), AbstractIndex)
    }
  }
```
这个Channel，有2个实现，一个是Java版本，另一个是C版本。JVM先使用Java版本，然后真正执行的是C版本。这2个Channel的实现分别是：

Java的FileChannelImpl:
```java
    public MappedByteBuffer map(MapMode mode, long position, long size)
        throws IOException
    {
        // 确保文件是打开的状态
        ensureOpen();

        // 忽略一堆判断
        ....

        try {
            beginBlocking();
            ti = threads.add();
            if (!isOpen())
                return null;

            long mapSize;
            int pagePosition;
            synchronized (positionLock) {
                long filesize;
                do {
                    filesize = nd.size(fd);
                } while ((filesize == IOStatus.INTERRUPTED) && isOpen());
                
                //又是各种判断,省略
                ....

                pagePosition = (int)(position % allocationGranularity);
                long mapPosition = position - pagePosition;
                mapSize = size + pagePosition;
                try {
                    //===================
                    // 创建一个文件映射
                    // 关注这里的map0方法
                    //===================
                    addr = map0(imode, mapPosition, mapSize);
                } catch (OutOfMemoryError x) {
                    // An OutOfMemoryError may indicate that we've exhausted
                    // memory so force gc and re-attempt map
                    System.gc();
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException y) {
                        Thread.currentThread().interrupt();
                    }
                    try {
                        addr = map0(imode, mapPosition, mapSize);
                    } catch (OutOfMemoryError y) {
                        // After a second OOME, fail
                        throw new IOException("Map failed", y);
                    }
                }
            } // synchronized

            //省咯文件描述对象操作
            ...
        } finally {
            threads.remove(ti);
            endBlocking(IOStatus.checkAll(addr));
        }
    }


    // -- Native methods --


    // 实际上，就是调用了一个本地方法。具体实现就在FileChannelImpl.c中实现的
    private native long map0(int prot, long position, long length)
        throws IOException;
```
那么C版本的FileChannelImpl.c:
```c
JNIEXPORT jlong JNICALL
Java_sun_nio_ch_FileChannelImpl_map0(JNIEnv *env, jobject this,
                                     jint prot, jlong off, jlong len)
{
    void *mapAddress = 0;
    jobject fdo = (*env)->GetObjectField(env, this, chan_fd);
    jint fd = fdval(env, fdo);
    int protections = 0;
    int flags = 0;

    if (prot == sun_nio_ch_FileChannelImpl_MAP_RO) {
        protections = PROT_READ;
        flags = MAP_SHARED;
    } else if (prot == sun_nio_ch_FileChannelImpl_MAP_RW) {
        protections = PROT_WRITE | PROT_READ;
        flags = MAP_SHARED;
    } else if (prot == sun_nio_ch_FileChannelImpl_MAP_PV) {
        protections =  PROT_WRITE | PROT_READ;
        flags = MAP_PRIVATE;
    }

    //========================
    // 直接调用了OS的系统调用函数
    //=========================
    mapAddress = mmap64(
        0,                    /* Let OS decide location */
        len,                  /* Number of bytes to map */
        protections,          /* File permissions */
        flags,                /* Changes are shared */
        fd,                   /* File descriptor of mapped file */
        off);                 /* Offset into file */

    if (mapAddress == MAP_FAILED) {
        if (errno == ENOMEM) {
            JNU_ThrowOutOfMemoryError(env, "Map failed");
            return IOS_THROWN;
        }
        return handle(env, -1, "Map failed");
    }

    return ((jlong) (unsigned long) mapAddress);
}

//=========================================
// 系统调用的mmap设置别名，表示为64位的mmap映射
//=========================================
#define mmap64 mmap
```
如果还有兴趣，可以看Linux内核源码，看看这个mmap如何实现映射的。