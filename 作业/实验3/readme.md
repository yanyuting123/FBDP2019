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