[toc]

# 实验3

## part 1

### 程序思路

程序由两次mapreduce完成。

第一次mapreduce统计了各省每个商品的关注量/购买量，输出key为province+count，value为item_id，但没有排序和筛选。

第二次mapreduce通过优先队列筛选了各省关注量/购买量前十的商品。

### 运行说明

将java源码打包成jar包，在系统节点上执行如下命令：

> bin/hadoop jar DescSort.jar \<input path> \<intermediate result path> \<result path> \<count type>

参数说明：

* input path：数据集的输入路径
* intermediate result path：第一次mapreduce的输出，程序的中间结果路径
* result path：结果的输出路径
* count type：concern 或 purchase，表示统计产品的关注量还是购买量

### 结果截图

结果顺序为province,count,item_id

各省商品关注量前十：

![](https://i.loli.net/2019/11/28/K1PCWwjmF74IEVx.png)

各省商品购买量前十：

![](https://i.loli.net/2019/11/28/r4UCqxRh7EsKHJM.png)

## part 2

```
//创建表
> create database experiment3;
> use experiment3;
> create external table doubleeleven(user_id int, item_id int, cat_id int, merchant_id int, brand_id int, month int, day int, action int, age_range int, gender int, province string)
row format delimited fields terminated by ',';
> load data local inpath '/home/yyt/Documents/HiveData/million_user_log.csv' overwrite into table doubleeleven;
//进行查询
//查询双11那天有多少人购买了商品：
> select count(distinct user_id) from doubleeleven where action=2;
//查询双11那天男女买家购买商品的比例：(比例为两次返回结果的比)
> select count(*) from doubleeleven where action=2 and gender=0;
> select count(*) from doubleeleven where action=2 and gender=1;
//查询双11那天浏览次数前十的品牌:
> from(select brand_id, count(*) as brand_count from doubleeleven group by brand_id) e select * order by e.brand_count desc limit 10;
from (select item_id, count(*) as countnum from doubleeleven where province='上海市' and action=2 group by item_id) e select * order by e.countnum desc limit 10;
```

### 结果截图

**查询双11那天有多少人购买了商品：**

![](https://i.loli.net/2019/11/27/gTAFDebl5h73tKv.png)

**查询双11那天男女买家购买商品的比例：(比例为两次返回结果的比)**

![](https://i.loli.net/2019/11/27/PqGsIcYwJd6NTZ7.png)

![](https://i.loli.net/2019/11/27/oXPxT1HCZJgcfaj.png)

男女比为38932：39058

**查询双11那天浏览次数前十的品牌:**

![](https://i.loli.net/2019/11/27/fxbp5LzJ6jKCEya.png)

### 一些操作记录

```
//创建外部表,注意这里的location是hdfs上的，或者先建表再导入数据也可以
> create external table doubleEleven(user_id int, item_id int, cat_id int, merchant_id int, brand_id int, month int, day int, action int, age_range int, gender int, province string)
row format delimited fields terminated by ','
location '/user/Documents/HiveData/million_user_log.csv';
```

#### 关于动态分区

```
//创建外部表并动态分区，似乎动态分区只能从原有的表中通过select创建
在hive-site.xml中添加设置：
<property>
	<name>hive.exec.dynamic.partition</name>
	<value>true</value>
	<description>enable dynamic partition</description>
</property>
<property>
	<name>hive.exec.dynamic.partition.mode</name>
	<value>nonstrict</value>
	<description>allow all partitions to be dynamic</description>
</property>
在hive shell中执行：
#province作为partition，故可以在新表中不创建这一列
> create table p_doubleeleven(user_id int, item_id int, cat_id int, merchant_id int, brand_id int, month int, day int, action int, age_range int, gender int) partitioned by (province string);
#多select的一项province超过了原来的列数，会自动作为partition的值,也就是多select的最后一项/多项会作为partition的值
> insert overwrite table p_doubleeleven partition(province) select user_id, item_id, cat_id, merchant_id, brand_id, month, day, action, age_range, gender, province from doubleeleven;
//可以并建议使用where字句在查询时对分区进行筛选，可以提高查询效率；不然如果分区过多还不进行筛选，会产生巨大的mapreduce任务
> select * from p_doubleeleven where province='上海市'
```

**一些配置**：

> set  hive.exec.max.dynamic.partitions.pernode=100 （默认100，一般可以设置大一点，比如1000）

表示每个maper或reducer可以允许创建的最大动态分区个数，默认是100，超出则会报错。

> set hive.exec.max.dynamic.partitions =1000(默认值) 

表示一个动态分区语句可以创建的最大动态分区个数，超出报错

> set hive.exec.max.created.files =10000(默认) 

全局可以创建的最大文件个数，超出报错。

### 一些坑

#### hive强制退出后报错SemanticException

因为手贱ctrl+z导致hive强行退出，然后再运行shell会报错

```
FAILED: SemanticException org.apache.hadoop.hive.ql.metadata.HiveException: java.lang.RuntimeException: Unable to instantiate org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient
```

【此方法会丢掉已有的database和table!!!】因为用的是derby数据库，进入hive目录，运行：

```
mv metastore_db metastore_db.tmp
bin/schematool -initSchema -dbType derby
```

对derby重新进行初始化

## part 3

### 环境

scala 2.12.10

java 1.8.0_222

hadoop 2.8.5

spark 2.4.4-without-hadoop-scala-2.12(通用hadoop版本适配，因为我用的是scala12所以选择了通过scala12编译的spark)

### scala安装和配置

`sudo apt install scala`默认安装的是scala11，但不知道为什么scala解释器里没办法显示已经键入的字符，就又卸载了，从官网安装了scala2.12.10。

1. 下载scala2.12.10.tgz，解压到想要安装到的文件夹。

2. 在*/etc/profile*添加配置:

   ```
   #scala
   export SCALA_HOME=/home/yyt/Apps/scala
   export PATH=${SCALA_HOME}/bin:$PATH
   ```

   刷新配置文件

   ```
   $ source /etc/profile
   ```

3. 检查是否安装成功

   ```
   $ scala -version
   Scala code runner version 2.12.10 -- Copyright 2002-2019, LAMP/EPFL and Lightbend, Inc.
   $ scala
   Welcome to Scala 2.12.10 (OpenJDK 64-Bit Server VM, Java 1.8.0_222).
   Type in expressions for evaluation. Or try :help.
   
   scala>
   ```

### spark standalone安装和配置

1. 从官网下载spark-2.4.4-bin-without-hadoop-scala-2.12.tgz，解压安装

2. 进入spark/conf文件夹，重命名文件：

   ```
   $ mv spark-env.sh.template spark-env.sh
   $ mv slaves.template slaves
   ```

3. 在*spark-env.sh*文件后面添加：

   ```
   export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
   export HADOOP_HOME=/home/yyt/hadoop-installs/hadoop
   export HADOOP_CONF_DIR=/home/yyt/hadoop-installs/hadoop/etc/hadoop
   export SCALA_HOME=/home/yyt/Apps/scala
   #可以使用hdfs
   export SPARK_DIST_CLASSPATH=$(/home/yyt/hadoop_installs/hadoop/bin/hadoop classpath)
   
   export SPARK_LOCAL_IP=localhost
   export SPARK_MASTER_IP=localhost
   export SPARK_WORKER_CORE=1  
   export SPARK_WORKER_INSTANCE=1  
     
   #export SPARK_WORKER_MEMORY=512M #每个worker最多可以使用多少内存  
   ```

   slaves文件中只有localhost。

4. 启动spark交互环境:

   ```
   $ bin/spark-shell
   2019-12-15 05:27:33,755 WARN  [main] util.NativeCodeLoader (NativeCodeLoader.java:<clinit>(62)) - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
   Setting default log level to "WARN".
   To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
   Spark context Web UI available at http://localhost:4040
   Spark context available as 'sc' (master = local[*], app id = local-1576416472136).
   Spark session available as 'spark'.
   
   Welcome to
         ____              __
        / __/__  ___ _____/ /__
       _\ \/ _ \/ _ `/ __/  '_/
      /___/ .__/\_,_/_/ /_/\_\   version 2.4.4
         /_/
            
   Using Scala version 2.12.8 (OpenJDK 64-Bit Server VM, Java 1.8.0_222)
   Type in expressions to have them evaluated.
   Type :help for more information.
   
   //退出：
   scala> :quit
   ```

5. 启动spark集群：

   ```
   $ sbin/start-all.sh
   ```

6. 查看启动的进程：

   ```
   $ jps
   14016 Jps
   13892 Worker
   13684 Master
   ```

提交到集群的命令格式：

```bash
bin/spark-submit --class WordCount /home/hadoop/WordCount.jar
```

### Spark+IntelliJ IDEA+Maven配置

1. 在启动界面的Configure - Plugins中（或在已有的项目界面中的File-Settings-Plugins），找到Scala，点击安装

2. 新建一个Maven项目，project SDK选择java 1.8

3. 在Project Structure - Ploatform Settings - Global Libraries中，添加scala SDK。

   添加好后右键点击添加的SDK，点击Copy to Project Libraries…，确认。

5. 在pom.xml中添加：

   ```xml
       <properties>
           <spark.version>2.4.4</spark.version>
           <scala.version>2.12</scala.version>
       </properties>
   
       <dependencies>
           <dependency>
               <groupId>org.apache.spark</groupId>
               <artifactId>spark-core_${scala.version}</artifactId>
               <version>${spark.version}</version>
           </dependency>
           <dependency>
               <groupId>org.apache.spark</groupId>
               <artifactId>spark-streaming_${scala.version}</artifactId>
               <version>${spark.version}</version>
           </dependency>
           <dependency>
               <groupId>org.apache.spark</groupId>
               <artifactId>spark-sql_${scala.version}</artifactId>
               <version>${spark.version}</version>
           </dependency>
           <dependency>
               <groupId>org.apache.spark</groupId>
               <artifactId>spark-hive_${scala.version}</artifactId>
               <version>${spark.version}</version>
           </dependency>
           <dependency>
               <groupId>org.apache.spark</groupId>
               <artifactId>spark-mllib_${scala.version}</artifactId>
               <version>${spark.version}</version>
           </dependency>
       </dependencies>
   
       <build>
           <plugins>
               <plugin>
                   <groupId>org.scala-tools</groupId>
                   <artifactId>maven-scala-plugin</artifactId>
                   <version>2.15.2</version>
                   <executions>
                       <execution>
                           <goals>
                               <goal>compile</goal>
                               <goal>testCompile</goal>
                           </goals>
                       </execution>
                   </executions>
               </plugin>
               <plugin>
                   <groupId>org.apache.maven.plugins</groupId>
                   <artifactId>maven-compiler-plugin</artifactId>
                   <configuration>
                       <source>1.8</source>
                       <target>1.8</target>
                   </configuration>
               </plugin>
               <plugin>
                   <groupId>org.apache.maven.plugins</groupId>
                   <artifactId>maven-surefire-plugin</artifactId>
                   <version>2.19</version>
                   <configuration>
                       <skip>true</skip>
                   </configuration>
               </plugin>
           </plugins>
       </build>
   ```
   
5. 在运行前要在Edit configuration 里将VM options设置为`-Dspark.master=local`，表示在本地运行（也可以设置为其他参数，表示在cluster上/yarn等框架上运行）。不然会报错：

   ```
   ERROR SparkContext: Error initializing SparkContext.
   org.apache.spark.SparkException: A master URL must be set in your configuration
   ```

   也可以在创建SparkConf或SparkSession对象时加上`.setMaster("local")`或`.master("local")`

##### 坑：

1. maven安装完dependency有时会有红色波浪下划线，可以通过Project Structure - Libraries找到对应的依赖位置，再到对应位置（默认是*~/.m2/repository/*）找到对应的依赖，手动删除后再刷新，重新安装一下就好了。

2. spark-shell 里运行val sc = new sparkContext(new sparkConf())会报错有多个sparkContext：

   ```
   org.apache.spark.SparkException: Only one SparkContext may be running in this JVM (see SPARK-2243). To ignore this error, set spark.driver.allowMultipleContexts = true. The currently running SparkContext was created at:
   org.apache.spark.sql.SparkSession$Builder.getOrCreate(SparkSession.scala:926)
   org.apache.spark.repl.Main$.createSparkSession(Main.scala:112)
   ```

   找了好久，发现原因是spark-shell里已经默认初始化了一个sparkContext对象，名称为sc，直接调用sc就可以了。另外也同样默认初始化了一个sparkSession对象，名称为spark。

### 实验过程

#### 统计各省销售最好的产品类别前十

CategoryTop.scala

将数据读取成rdd，filter筛选出表示购买的数据，map出( (province,category_id) , 1)的kv对，然后进行reduceByKey求value和，统计出每个省份每个类别的销售量，然后根据（省份，销售量）倒序排序。

然后构造一个空的Map用来筛选各省前十，key为省份，value为该省份已筛选出的产品类别数。设计函数来筛选各省前十，每筛选出一个该省份的数据，value+1，value小于10时表示该省份尚未选出前十（遍历10次），返回true来选择该条数据，value>=10时表示该省份已选出前十，返回false不选择该条数据。调用该函数，用filter筛选出各省前十。

**实验截图:**

顺序为 （（省份，产品类别），销售量）

![](https://i.loli.net/2019/12/24/f2Z5oaxIdCSBPVy.png)

#### 统计各省的双十一前十热门销售产品

ProductPurchaseTop.scala

过程同上，将category_id换成item_id

**实验截图:**

顺序为 （（省份，产品id），销售量）

![](https://i.loli.net/2019/12/24/svhSQ2jmxBwZAtD.png)

#### 查询双11那天浏览次数前十的品牌

BrandViewTop10.scala

用SparkSQL中的dataframe读取csv数据，groupBy品牌并count（要先调整为1个分区，否则会在各个分区内进行groupBy和count，有的品牌id会出现在多个分区里，会影响结果的统计；当然在各个分区内先各自统计，再调整为1个分区，再统计一次也可以）。

然后根据count值倒序排序取前10就好啦。

**实验截图:**

顺序为 品牌，浏览数

![](https://i.loli.net/2019/12/24/7CBQwYjgR4Ga96q.png)

## part4

### 数据预处理

只保留label为0或1的数据，并去掉空值

```
# read data
val df = spark.read.csv("file:///home/yyt/Documents/Data/train.csv")
val columns = Seq("user_id","age_range","gender","merchant_id","label")
val train = df.toDF(columns:_*)
# filter the label and drop null value
val train2 = train.filter($"label".equalTo("0") || $"label".equalTo("1"))
val train3 = train2.na.drop().repartition(1)
# write data
train3.write.csv("file:///home/yyt/Documents/Data/train")
```

将处理后的文件命名为trainData.csv

训练过程中将该数据三七分为测试数据和训练数据。

### 预测回头客

PredictReturnedCustomer.scala

结果保存在PredictReturnedCustomer文件夹里。

因为似乎以后的Spark版本会转向ML库，MLlib已经进入维护阶段不再更新了，而且pipeline确实是个好东西，一开始打算用ML库。后来发现ML库的二分类评估器功能实在太少了……（只支持areaUnderROC和areaUnderPR）最后用ML实现了数据训练，用MLlib的评估器做了评估。

评估结果很差很差……:cry:,有尝试调参但是无果。可能和数据label太不均匀也有关……

**分类器**:

**LogisticRegression:**

> Coefficients:
> -7.112198112944368E-8  0.035907683998692465  -0.12032356083020809  6.629859887087145E-6  
> Intercept:[-2.795798552427288]
> numClasses:2
> numFeatures:4

**DecisionTree:**

决策树训练出的结果就是直接预测0……:sweat:

> Tree:
> DecisionTreeClassificationModel (uid=dtc_f08dccef20cc) of depth 0 with 1 nodes
>   Predict: 0.0

**RandomForest:**

> Tree:
> RandomForestClassificationModel (uid=rfc_75739293cc29) with 20 trees
>   Tree 0 (weight 1.0):
>     Predict: 0.0
>   Tree 1 (weight 1.0):
>     Predict: 0.0
>   Tree 2 (weight 1.0):
>     If (feature 1 <= 3.5)
>      Predict: 0.0
>     Else (feature 1 > 3.5)
>      If (feature 2 <= 1.5)
>       Predict: 0.0
>      Else (feature 2 > 1.5)
>       If (feature 3 <= 989.0)
>        If (feature 1 <= 7.5)
>         Predict: 0.0
>        Else (feature 1 > 7.5)
>         If (feature 3 <= 173.5)
>          Predict: 0.0
>         Else (feature 3 > 173.5)
>          Predict: 1.0
>       Else (feature 3 > 989.0)
>        Predict: 0.0
>   Tree 3 (weight 1.0):
>     If (feature 1 <= 3.5)
>      If (feature 2 <= 0.5)
>       Predict: 0.0
>
> ……

**LSVM:**

> Coefficients:
> [0.0,8.761494426689734E-9,-9.719342719910127E-9,-1.6835425818886377E-12]
> Intercept:-1.0000000707130128
> numClasses:2
> numFeatures:4

**GradientBoostingTree:**

> Tree:
> GBTClassificationModel (uid=gbtc_bddfaa6ba36f) with 20 trees
>   Tree 0 (weight 1.0):
>     If (feature 1 <= 3.5)
>      If (feature 2 <= 0.5)
>       If (feature 3 <= 4761.0)
>        If (feature 3 <= 4045.5)
>         If (feature 3 <= 3676.0)
>          Predict: -0.8826768642447419
>         Else (feature 3 > 3676.0)
>          Predict: -0.8582375478927203
>        Else (feature 3 > 4045.5)
>         If (feature 3 <= 4159.5)
>          Predict: -0.9165894346617238
>         Else (feature 3 > 4159.5)
>          Predict: -0.8900313886784283
>       Else (feature 3 > 4761.0)
>        If (feature 0 <= 261327.0)
>         If (feature 0 <= 235267.5)
>          Predict: -0.8558558558558559
>         Else (feature 0 > 235267.5)
>          Predict: -0.786046511627907
>        Else (feature 0 > 261327.0)
>         If (feature 0 <= 401331.0)
>
> ……

**评估结果**:

Accuracy（正确预测数据/所有数据）:

感觉NaiveBayes比较差也情有可原？Spark的NaiveBayes不是针对连续性变量的，默认是多项式模型。其它模型一样好的原因是……训练出来所有的预测都是０……:sweat:

![](https://i.loli.net/2019/12/24/YMWaPoky6bOTpu3.png)

ML库的LSVM不知道为什么不会给出预测的score(probability)，没办法进行进一步评估↓

ROC：

决策树的ROC是（0，0）（1，1），只有

![](https://i.loli.net/2019/12/24/DypVFAWwl9YEjSR.png)

area under ROC:

实际上只有NaiveBayes超过了0.5……

![](https://i.loli.net/2019/12/24/xbwynN93kBSrjcH.png)

PR:

这个PR曲线也让人有点怀疑人生……按理来说随着recall增加precision应该随之下降。

![](https://i.loli.net/2019/12/24/RXA1DEO3PtTLNsz.png)

areaUnderPR:

![](https://i.loli.net/2019/12/24/YzdEHPMCtO6clpZ.png)