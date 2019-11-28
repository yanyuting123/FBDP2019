# 作业7

1. 简述为什么会有Spark

   * Hadoop计算框架对很多非批处理大数据问题有局限性，人们开始关注大数据处理的其他计算模式和系统。

   * spark基于内存计算，提高了计算性能。
     * 提出了基于内存的弹性分布式数据集RDD，一组RDD形成可执行的DAG，构成灵活的计算流图
     * 某些场景下比Hadoop快100倍
   * spark支持多种语言的API，包括Java, Scala, Python, R, SQL。
   * 应用场景广，SQL、Streaming、MLlib、GraphX提供了多种应用场景支持。
   * 可基于Hadoop，Apache Mesos， Kubernetes，standalone等模式运行，兼容性强。

2. 对比Hadoop和Spark

   特点对比：

   * Spark可以把中间数据放在内存里，迭代运算效率高。Hadoop的中间数据需要写入文件系统，效率低。
   * Spark容错性高，它引入了RDD的抽象，即一组节点上的弹性的只读对象集合，在RDD计算时可以通过Checkpoint来实现容错，Checkpoint有两种方式：Checkpoint Data和Logging The Updates。
   * Spark更加通用。Spark提供的数据集操作类型更多，各个处理节点之间的通信模型也不只有Shuffle，用户可以命名、物化、控制中间结果的存储、分区等。Hadoop只有Map和Reduce两种操作。
   * Spark对迭代更加友好，更适用于迭代的机器学习场景。
   * Spark基于RDD可进行更加多样、一般化的计算，Hadoop只基于MapReduce框架计算。
   * Hadoop可用来做分布式数据存储+分布式计算，Spark只用来做分布式计算。

   生态圈比较：

   * 批处理：Hadoop用MapReduce，Spark用Spark RDDs
   * SQL查询：Hadoop用Hive，Spark用Spark SQL
   * 流处理/实时处理：Hadoop用Storm Kafka，Spark用Spark Streaming
   * 机器学习：Hadoop用Mahout，Spark用Spark ML Lib
   * 实时查询：Hadoop用Hbase、Cassandra等等，Spark没有直接的组件，但可以查询NoSQL存储的数据。

   优势比较：

   * Hadoop的优势：
     * 多数据源、多应用、多用户
     * 有可靠性、多租户、有安全性
     * 应用广，包括文件、数据库、半结构化数据
   * Spark的优势：
     * API更简单，支持Python、Scala、Java
     * 基于RDD和有向无环图的处理，使用内存存储，提升了性能
     * 可合并工作流，处理各种场景，如使用Shark、ML Streaming、GraphX。

3. 简述Spark的技术特点
   * RDD：Spark提出的弹性分布式数据集，是Spark最核心的分布式数据抽象。
   * Transformation&Action：Spark通过RDD的两种不同类型的运算实现了惰性计算，在RDD的Transformation运算时，Spark并没有进行作业的提交；而在RDD的Action操作时才会触发SparkContext提交作业。
   * Lineage：为了保证数据鲁棒性，Spark通过Lineage记录一个RDD是如何通过其他一个或者多个父类RDD转变过来的，当这个RDD的数据丢失时，可以通过它父类的RDD重新计算。
   * Spark调度：采用事件驱动的Scala库类Akka来完成任务启动，通过复用线程池的方式来取代MapReduce进程或者线程启动和切换的开销。
   * API：Spark使用Scala开发，并且默认Scala作为其编程语言。因此编写Spark程序比MapReduce程序简洁得多。同时也支持Java、Python语言进行开发。
   * Spark生态： Spark SQL、Spark Streaming、GraphX等为Spark的应用提供了丰富的场景和模型，适用于不同的计算模式和计算任务。
   * Spark部署：拥有Standalone、Mesos、YARN等多种部署方式，可以部署在多种底层平台上。
   * 适用性：适用于需要多次操作特定数据集的应用场合。需要反复操作的次数越多，所需读取的数据量越大，收益越大。数据量小但是计算密集度较大的场合受益就相对小。不适用于异步细粒度更新状态的应用，例如web服务的存储或者是增量的web爬虫和索引。不适合增量修改的应用模型。
   * Spark是一种基于内存的迭代式分布式计算框架，适合于完成一些迭代式、关系查询、流式处理等计算密集型任务。