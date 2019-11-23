# HBase安装和使用

## 环境

vmware-ubuntu18.04

这里hbase2.2.2自带的lib是hadoop2.8.5，所以从官网重装一个hadoop2.8.5。（我试图使用hadoop3.1.2，尝试了很久之后宣告失败）

hbase2.2.2

jdk1.8

## 单机模式

按官网 http://hbase.apache.org/book.html 的步骤来，此前hadoop路径和java路径的环境变量都已经配置好了

1. 在hbase中配置Java路径，在*hbase/conf/hbase-env.sh*中添加：

   ```
   export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
   ```

2.  在*conf/hbase-site.xml*添加配置：

   ```xml
   <configuration>
     <property>
       <name>hbase.rootdir</name>
       <value>file:///home/testuser/hbase</value>
     </property>
     <property>
       <name>hbase.zookeeper.property.dataDir</name>
       <value>/home/testuser/zookeeper</value>
     </property>
     <property>
       <name>hbase.unsafe.stream.capability.enforce</name>
       <value>false</value>
       <description>
         Controls whether HBase will check for stream capabilities (hflush/hsync).
   
         Disable this if you intend to run on LocalFileSystem, denoted by a rootdir
         with the 'file://' scheme, but be mindful of the NOTE below.
   
         WARNING: Setting this to false blinds you to potential data loss and
         inconsistent system state in the event of process and/or node failures. If
         HBase is complaining of an inability to use hsync or hflush it's most
         likely not a false positive.
       </description>
     </property>
   </configuration>
   ```

3. 开启hbase服务，输入

   ```
   bin/start-hbase.sh
   ```

   开启hbase服务，可以通过jps观察是否开启，并通过默认web端口 *[http://localhost:16010](http://localhost:16010/)* 来查看Web UI。

4. 开启hbase shell，输入命令：

   ```
   $./bin/hbase shell
   hbase(main):001:0>
   ```

   成功显示则已经进入hbase shell

5. 试用一下hbase shell：

   ```python
   # create a table
   create 'test', 'cf'
   # list information
   list 'test'
   # see details, including configuration defaults
   describe 'test'
   # put data into table
   put 'test', 'row1', 'cf:a', 'value1'
   put 'test', 'row2', 'cf:b', 'value2'
   # scan the table for all data at once
   scan 'test'
   # get a single row of data
   get 'test', 'row1'
   # disable and enable a table
   disable 'test'
   enable 'test'
   # drop(delete) a table(you should first disable it before drop it)
   drop 'test'
   # exit the HBase Shell
   quit
   ```

6. 关闭hbase

   ```
   ./bin/stop-hbase.sh
   ```

#### 运行截图

![](https://i.loli.net/2019/11/19/8I1B4xVrYeApsG2.png)

## 伪分布模式

### 失败的尝试

试图使用hadoop3.1.2，这里选择3.1.2是因为原来装的是3.0.0，查了hbase官网手册，发现hbase没有任何版本支持3.0.0-3.0.2……但是最新的测试显示hbase2.2.x版本支持hadoop3.1.1+，以及对hadoop3.x.x版本的迷之执着，就从官网重新安装了hadoop3.1.2版本。

#### 配置hbase/lib中的hadoop依赖包

因为我使用的的hbase中lib文件夹下hadoop相关包的版本和我hadoop的版本不同，要手动配置hbase依赖的hadoop包。

手动参照*hbase/lib*下hadoop-\*-2.8.5的jar包，在*hadoop/share/hadoop*中找到对应的jar包，拷贝过去，并把原来版本的删除。可以使用`find -name <filename regex>`来辅助寻找文件位置。其中hadoop-client-2.8.5.jar和hadoop-minicluster-2.8.5.jar这两个文件在hadoop的jar包里没找到，只能自己想办法：

* 从官网下载hadoop-3.1.2-src，发现对应的hadoop-client和hadoop-minicluster文件夹里都只有一个pom.xml文件，而打开hadoop-client-2.8.5.jar和hadoop-minicluster-2.8.5.jar这两个包也只有配置说明，按照对应的格式自己生成hadoop-client-3.1.2.jar和hadoop-minicluster-3.1.2.jar，放进hbase的lib文件夹中。

**坑1**：不知道为什么操作完后hbase启动会报错NoClassDefFoundError，网上说是因为jar包不全，于是将hadoop/share/hadoop下所有jar包都导入hbase/lib中，解决：

```
find /home/yyt/hadoop_installs/hadoop/share/hadoop -name "hadoop*jar" | xargs -i cp {} /home/yyt/hadoop_installs/hbase/lib/
```

**坑2**：hbase启动后，打开hbase shell操作时显示Master is initializing，而且web显示region陷入了永久
RIT……折腾了很久都没有解决。可能是我伪造jar包的过程有问题，或者是版本不匹配。最终宣告失败。

### 成功的尝试

于是向hbase低头，重新安装hadoop2.8.5（我再也不作死了qwq）

记录下伪分布配置过程：

1. 将*hbase-site.xml*中的配置改为：

   ```xml
   <configuration>
     <property>
       <name>hbase.rootdir</name>
   	<value>hdfs://localhost:9000/hbase</value>
     </property>
     <property>
       <name>hbase.zookeeper.property.dataDir</name>
       <value>/home/yyt/hadoop_installs/zookeeper</value>
     </property>
     <property>
       <name>hbase.unsafe.stream.capability.enforce</name>
       <value>true</value>
       <description>
         Controls whether HBase will check for stream capabilities (hflush/hsync).
   
         Disable this if you intend to run on LocalFileSystem, denoted by a rootdir
         with the 'file://' scheme, but be mindful of the NOTE below.
   
         WARNING: Setting this to false blinds you to potential data loss and
         inconsistent system state in the event of process and/or node failures. If
         HBase is complaining of an inability to use hsync or hflush it's most
         likely not a false positive.
       </description>
     </property>
     <property>
       <name>hbase.cluster.distributed</name>
       <value>true</value>
     </property>
     <property>
       <name>hbase.zookeeper.quorum</name>
       <value>localhost:2181</value>
     </property>
   </configuration>
   ```

2. 重新开启hbase，jps中除了hadoop守护程序，还要有`HMaster`, `HRigionServer`,`HQuorumPeer`这三个，代表hbase和zookeeper都已开启。

3. 打开hbase shell进行测试

   ```
   $ bin/start-hbase.sh
   $ bin/hbase shell
   ```

   能够成功执行建立表格、添加数据等操作就可以了。

## Intellij IDEA Maven对应配置

在pom.xml中添加dependency:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>HadoopPractice</groupId>
    <artifactId>HadoopPractice</artifactId>
    <version>1.0-SNAPSHOT</version>

    <!--automatically add xml files and properties files in resources catalog to target catalog-->
    <build>
        <resources>
            <resource>
                <directory>src/main/resource</directory>
                <includes>
                    <include>**/*.xml</include>
                    <include>**/*.properties</include>
                </includes>
            </resource>
        </resources>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                </configuration>
            </plugin>
        </plugins>
    </build>
    <repositories>
        <repository>
            <id>apache</id>
            <url>http://maven.apache.org</url>
        </repository>
    </repositories>
    <dependencies>
        <!-- https://mvnrepository.com/artifact/junit/junit -->
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.11</version>
            <scope>test</scope>
        </dependency>
        <!--&lt;!&ndash; https://mvnrepository.com/artifact/commonslogging/commons-logging &ndash;&gt;-->
        <dependency>
            <groupId>commons-logging</groupId>
            <artifactId>commons-logging</artifactId>
            <version>1.1.3</version>
        </dependency>
        <!--&lt;!&ndash;
        https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
        &ndash;&gt;-->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>2.8.5</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-hdfs</artifactId>
            <version>2.8.5</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>2.8.5</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-client</artifactId>
            <version>2.2.2</version>
        </dependency>
    </dependencies>
</project>
```
#### 运行截图

![](https://i.loli.net/2019/11/23/s3nYiPI9JkSDBQZ.png)

![](https://i.loli.net/2019/11/23/Ts38xROfn2hHw7K.png)
