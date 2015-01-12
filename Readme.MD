o
flume 数据采集；
kafka 大数据的mq,用于实时数据传递；
storm 实时数据传递；

kafka:测试ok
Kafka是一个高吞吐量分布式消息系统。linkedin开源的kafka。 Kafka就跟这个名字一样，设计非常独特。首先，kafka的开发者们认为不需要在内存里缓存什么数据，操作系统的文件缓存已经足够完善和强大，只要你不搞随机写，顺序读写的性能是非常高效的。kafka的数据只会顺序append，数据的删除策略是累积到一定程度或者超过一定时间再删除。Kafka另一个独特的地方是将消费者信息保存在客户端而不是MQ服务器，这样服务器就不用记录消息的投递过程，每个客户端都自己知道自己下一次应该从什么地方什么位置读取消息，消息的投递过程也是采用客户端主动pull的模型，这样大大减轻了服务器的负担。Kafka还强调减少数据的序列化和拷贝开销，它会将一些消息组织成Message Set做批量存储和发送，并且客户端在pull数据的时候，尽量以zero-copy的方式传输，利用sendfile（对应java里的 FileChannel.transferTo/transferFrom）这样的高级IO函数来减少拷贝开销。可见，kafka是一个精心设计，特定于某些应用的MQ系统，这种偏向特定领域的MQ系统我估计会越来越多，垂直化的产品策略值的考虑。
http://www.infoq.com/cn/articles/apache-kafka

        1.先启动zookeeper服务，端口确保2181.        
        2.kafka-server-start.sh /usr/local/etc//kafka/server.properties

        3.创建一个主题

        创建，kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test 

        获取，kafka-topics.sh --list --zookeeper localhost:2181 

        4.发送一些消息: 在控制台输入消息

        kafka-console-producer.sh --broker-list localhost:9092 --topic test 

        5.启动一个消费者：观察在数据台显示消息

        kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning





flume:数据采集
Flume 是一个分布式、可靠、和高可用的海量日志采集、聚合和传输的系统。



启动flume
cd /Applications/apache-flume-1.5.0-bin/

flume:source 采用nginx log ok
#bin/flume-ng agent --conf conf --conf-file conf/flume.conf.nginx --name a1 -Dflume.root.logger=INFO,console
     启动nginx,访问，观察界面日志输出。ok。

#bin/flume-ng agent --conf conf --conf-file conf/flume.conf.hbase --name a1 -Dflume.root.logger=INFO,console
     启动hbase环境，观察hbase的数据变化。

#bin/flume-ng agent --conf conf --conf-file conf/flume.conf.kafka --name producer -Dflume.root.logger=INFO,console

hadoop hive and base:环境配置（之前已经进行过hadoop基础环境的配置）

1.hadoop环境配置测试
hdfs  namenode -format
start-all.sh  //注意端口9000  与php-fpm端口冲突
jps  
cd /usr//local/Cellar/hadoop/2.6.0/libexec/share/hadoop/mapreduce/
hadoop jar hadoop-mapreduce-examples-2.6.0.jar pi 10 100
stop-all.sh
jps

2.hbase
hbase 单机模式
hbase-site.xml
<configuration>
<property>
     <name>hbase.rootdir</name>
     <value>file:/usr/local/opt/hbase</value>
</property>
<property>
     <name>hbase.zookeeper.property.dataDir</name>
     <value>file:/usr/local/opt/zookeeper/</value>
</property>
</configuration>

stop-all.sh
stop-zookeeper
启动单机hbase,数据进入hbase
测试未完成。。。

集群模式
<configuration>
     <property>
          <name>hbase.master</name>
          <value>localhost:9000</value>
     </property>
     <property>
          <name>hbase.rootdir</name>
          <value>hdfs://localhost:9000/hbase</value>
     </property>
     <property>
          <name>hbase.zookeeper.quorum</name>
          <value>localhost</value>
     </property>
     <property>
          <name>hbase.cluster.distributed</name>
          <value>true</value>
     </property>
     <property>
          <name>hbase.zookeeper.property.maxClientCnxns</name>
          <value>300</value>
     </property>
</configuration>


start-hbase.sh
hbase shell
     >create ‘xyz', ‘f1'
     >describe 'xyz'
     >scan 'xyz'
     >put 'xyz','100’,'cf1:val','www.360buy.com'

create 'test_table', 'stats'
put 'test_table', 'c41e4:1:20130201', 'stats:count', 100
put 'test_table', 'c41e4:1:20130202', 'stats:count', 102
put 'test_table', 'ed516:2:20130201', 'stats:count', 80
scan 'test_table', {LIMIT=>25}

测试ok

disable 'hbase_table_1'
drop  ‘hbase_table_1'


> create 't1', ‘f1';
hbase(main):002:0> describe "xyz"
hbase(main):003:0> scan “xyz"
>put 'xyz','100','cf1:val','www.360buy.com'
删除一条数据
deleteall 'hbase_table_3','100'


3.hive
     brew install hive
hive语法
http://www.cnblogs.com/tangtianfly/archive/2012/06/28/2567376.html

4.hbase and hive 整合

hbase and hive整合（http://www.sqlparty.com/hive%E4%B8%8Ehbase%E6%95%B4%E5%90%88/）
http://www.blogjava.net/ivanwan/archive/2011/01/21/343346.html

以上整合过程和操作步骤已经执行完毕，现在Hive中添加记录HBase中有记录添加，同样你在HBase中添加记录Hive中也会添加， 表示Hive与HBase整合成功，对海量级别的数据我们是不是可以在HBase写入，在Hive中查询 喃？因为HBase 不支持复杂的查询，但是HBase可以作为基于 key 获取一行或多行数据，或者扫描数据区间，以及过滤操作。而复杂的查询可以让Hive来完成，一个作为存储的入口(HBase)，一个作为查询的入口(Hive)。如下图示。

补充运算确实的jar包
hdfs dfs -mkdir hdfs://localhost:9000/usr/local/Cellar/hive/0.13.1/libexec/lib
hdfs dfs -put /usr/local/Cellar/hive/0.13.1/libexec/lib/hive-hbase-handler-0.13.1.jar hdfs://localhost:9000/usr/local/Cellar/hive/0.13.1/libexec/lib/
hdfs dfs -put /usr/local/Cellar/hive/0.13.1/libexec/lib/hbase-client-0.98.6.1-hadoop2.jar hdfs://localhost:9000/usr/local/Cellar/hive/0.13.1/libexec/lib/

hdfs dfs -mkdir hdfs://localhost:9000/usr/local/Cellar/hbase/0.98.6.1/libexec/lib/
hdfs dfs -put /usr/local/Cellar/hbase/0.98.6.1/libexec/lib/hbase-server-0.98.6.1-hadoop2.jar hdfs://localhost:9000/usr/local/Cellar/hbase/0.98.6.1/libexec/lib/
hdfs dfs -put /usr/local/Cellar/hbase/0.98.6.1/libexec/lib/htrace-core-2.04.jar hdfs://localhost:9000/usr/local/Cellar/hbase/0.98.6.1/libexec/lib/
hdfs dfs -put /usr/local/Cellar/hbase/0.98.6.1/libexec/lib/hbase-protocol-0.98.6.1-hadoop2.jar hdfs://localhost:9000/usr/local/Cellar/hbase/0.98.6.1/libexec/lib/
hdfs dfs -put /usr/local/Cellar/hbase/0.98.6.1/libexec/lib/hbase-common-0.98.6.1-hadoop2.jar hdfs://localhost:9000/usr/local/Cellar/hbase/0.98.6.1/libexec/lib/

hdfs dfs -mkdir hdfs://localhost:9000/usr/local/Cellar/hadoop/2.6.0/libexec/share/hadoop/common/

hdfs dfs -put /usr/local/Cellar/hadoop/2.6.0//libexec/share/hadoop/common/hadoop-common-2.6.0.jar  hdfs://localhost:9000/usr/local/Cellar/hadoop/2.6.0/libexec/share/hadoop/common/
hdfs dfs -put /usr/local/Cellar/hadoop/2.6.0//libexec/share/hadoop/common/lib/protobuf-java-2.5.0.jar  hdfs://localhost:9000/usr/local/Cellar/hadoop/2.6.0/libexec/share/hadoop/common/lib/
hdfs dfs -put /usr/local/Cellar//hbase/0.98.6.1/libexec/lib/high-scale-lib-1.1.1.jar hdfs://localhost:9000/usr/local/Cellar/hbase/0.98.6.1/libexec/lib/
hdfs dfs  -put /usr/local/Cellar//hive/0.13.1/libexec/lib/hive-exec-0.13.1.jar hdfs://localhost:9000/usr/local/Cellar/hive/0.13.1/libexec/lib/
hdfs dfs -put /usr/local/Cellar//hadoop/2.6.0/libexec/share/hadoop/common/lib/zookeeper-3.4.6.jar  hdfs://localhost:9000/usr/local/Cellar/hadoop/2.6.0/libexec/share/hadoop/common/lib/

hdfs dfs -put /usr/local/Cellar//hadoop/2.6.0/libexec/share/hadoop/common/lib/netty-3.6.2.Final.jar hdfs://localhost:9000/usr/local/Cellar/hadoop/2.6.0/libexec/share/hadoop/common/lib
hdfs dfs -put /usr/local/Cellar//hbase/0.98.6.1/libexec/lib/hbase-hadoop-compat-0.98.6.1-hadoop2.jar hdfs://localhost:9000/usr/local/Cellar/hbase/0.98.6.1/libexec/lib/
hdfs dfs -put /usr/local/Cellar//hadoop/2.6.0/libexec/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.6.0.jar hdfs://localhost:9000/usr/local/Cellar/hadoop/2.6.0/libexec/share/hadoop/mapreduce/
hdfs dfs -put /usr/local/Cellar//hadoop/2.6.0/libexec/share/hadoop/common/lib/guava-11.0.2.jar hdfs://localhost:9000/usr/local/Cellar/hadoop/2.6.0/libexec/share/hadoop/common/lib/


现在你可以使用hive命令启动hive，在后端的HBase上创建一张表。例子中的表名为test，有一个叫values的整数的列簇（Cloumn Family）。注意对表的删除/创建只会影响Hive的元数据；并没有真正在HBase生效。

//在hive中创建hbase数据表，并且保持同步

DROP TABLE IF EXISTS hbase_table_1;

CREATE TABLE hbase_table_1(key int, value string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:val")
TBLPROPERTIES ("hbase.table.name" = "xyz");

CREATE TABLE pokes (foo INT, bar STRING);
LOAD DATA LOCAL INPATH '/usr/local/Cellar/hive/0.13.1/libexec/examples/files/kv1.txt' OVERWRITE INTO TABLE pokes;
INSERT OVERWRITE TABLE hbase_table_1 SELECT foo,bar FROM pokes WHERE foo=90;

select * from hbase_table_1;
切换到hive
scan ‘xyz'
//经过测试数据同时出现在hive and hbase里面.


hive访问已经存在的hbase
CREATE EXTERNAL TABLE hbase_table_2(key int, value string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = "cf1:val")
TBLPROPERTIES("hbase.table.name" = "some_existing_table”);

多列和多列族
CREATE TABLE hbase_table_3(key int, value1 string, value2 int, value3 int)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = ":key,a:b,a:c,d:e"
);
INSERT OVERWRITE TABLE hbase_table_3 SELECT foo, bar, foo+1, foo+2
FROM pokes WHERE foo=98 OR foo=100;

hive不做压缩的，只是在hdfs中移动数据，或是从本地文件系统移动到hdfs。原来是多大就是多大。
如果要压缩，可以先压缩好再导入，hive是不会替你做这步的。hive支持gz格式和lzo格式。gz格式原生支持。lzo格式需要某个特殊的serde。

整合flume and hbase

CREATE TABLE foo_table3 (key int, value string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,bar_cf:payload");

flume.conf.hbase
a1.sinks.k1.type = hbase
a1.sinks.k1.table = foo_table3
a1.sinks.k1.columnFamily = bar_cf
a1.sinks.k1.serializer = org.apache.flume.sink.hbase.RegexHbaseEventSerializer
a1.sinks.k1.channel = c1

cd /Applications/apache-flume-1.5.0-bin/
#bin/flume-ng agent --conf conf --conf-file conf/flume.conf.hbase --name a1 -Dflume.root.logger=INFO,console


浏览器浏览127.0.0.1地址，观察hbase的数据变化。
hbase(main):021:0> scan 'foo_table3'
ROW COLUMN+CELL
1421051660964-1ludqgImWZ-0 column=bar_cf:payload, timestamp=1421051664127, value=127.0.0.1 - - [12/Jan/2015:16:3
4:20 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_
1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
1421051666626-1ludqgImWZ-1 column=bar_cf:payload, timestamp=1421051669633, value=127.0.0.1 - - [12/Jan/2015:16:3
4:22 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_
1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
1421051732200-1ludqgImWZ-2 column=bar_cf:payload, timestamp=1421051735206, value=127.0.0.1 - - [12/Jan/2015:16:3
5:32 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_
1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"

hive> select * from foo_table3;
NULL 127.0.0.1 - - [12/Jan/2015:16:34:20 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
NULL 127.0.0.1 - - [12/Jan/2015:16:34:22 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
NULL 127.0.0.1 - - [12/Jan/2015:16:35:32 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
NULL 127.0.0.1 - - [12/Jan/2015:16:35:35 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
Time taken: 0.064 seconds, Fetched: 4 row(s)


整合 flume and kafka ;

flume抓取数据送到kafka
#cd /Applications/apache-flume-1.5.0-bin/
#bin/flume-ng agent --conf conf --conf-file conf/flume.conf.kafka --name producer -Dflume.root.logger=INFO,console

kafka观察数据变化情况
kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning

////////
127.0.0.1 - - [12/Jan/2015:16:34:20 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
127.0.0.1 - - [12/Jan/2015:16:34:22 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
127.0.0.1 - - [12/Jan/2015:16:35:19 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
127.0.0.1 - - [12/Jan/2015:16:35:32 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
127.0.0.1 - - [12/Jan/2015:16:35:34 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
127.0.0.1 - - [12/Jan/2015:16:35:35 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
127.0.0.1 - - [12/Jan/2015:16:42:32 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
127.0.0.1 - - [12/Jan/2015:16:42:46 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
//////////

启动kafka,观察到数据进入到kafka kafka可以作为省份数据传递的工具，类似于中国银行传递数据使用mq
通过访问nginx,产生的日志，可以通过kafka的观察器观察到



采集数据3台服务器web日志到hdfs,将来进行离线运算，取代rsync；采集数据，进行实时计算。
频道，前台 后台 接口日志；每台服务器定义多个。多台机器前台日志，聚集到一个频道，到mq 和 hbase


storm配置
Storm的主要特点如下：
简单的编程模型。类似于MapReduce降低了并行批处理复杂性，Storm降低了进行实时处理的复杂性。
可以使用各种编程语言。你可以在Storm之上使用各种编程语言。默认支持Clojure、Java、Ruby和Python。要增加对其他语言的支持，只需实现一个简单的Storm通信协议即可。
容错性。Storm会管理工作进程和节点的故障。
水平扩展。计算是在多个线程、进程和服务器之间并行进行的。
可靠的消息处理。Storm保证每个消息至少能得到一次完整处理。任务失败时，它会负责从消息源重试消息。
快速。系统的设计保证了消息能得到快速的处理，使用ØMQ作为其底层消息队列。（0.9.0.1版本支持ØMQ和netty两种模式）
本地模式。Storm有一个“本地模式”，可以在处理过程中完全模拟Storm集群。这让你可以快速进行开发和单元测试。

主节点：主节点通常运行一个后台程序 —— Nimbus ，用于响应分布在集群中的节点，分配任务和监测故障。
工作节点： Supervisor , 负责接受nimbus 分配的任务，启动和停止属于自己管理的worker进程。Nimbus和Supervisor之间的协调由zookeeper完成。
Worker ：处理逻辑的进程，在其中运行着多个Task ，每个task 是一组spout/blots的组合。
Topology : 是storm 的实时应用程序，从启动开始一直运行，只要有tuple过来 就会触发执行。拓扑：storm的消息流动很像一个拓扑结构。
2. stream是storm的核心概念，一个stream是一个持续的tuple序列，这些tuple被以分布式并行的方式创建和处理。
3. spouts是一个stream的源头，spouts负责从外部系统读取数据，并组装成tuple发射出去，tuple被发射后就开始再topology中传播。
4. bolt是storm中处理 数据的核心，storm中所有的数据处理都是在bolt中完成的


brew install storm
brew upgrade storm
1）先启动zookeeper，sh /usr/local/zookeeper/bin/zkServer start
2) 启动nimbus（主节点）：storm nimbus
启动supervisor（从节点）:storm supervisor
启动ui：storm ui


观察storm集群下面的状态storm ui的访问地址  http://127.0.0.1:8080/index.html
cd /Applications/apache-storm-0.9.2-incubating/examples/storm-starter
storm jar  storm-starter-topologies-0.9.2-incubating.jar storm.starter.WordCountTopology test

本地线程模拟运行
在本机模式下，用线程模拟storm平台执行topology
storm jar target/storm-starter-0.9.2-incubating-jar-with-dependencies.jar storm.starter.WordCountTopology



大数据架构：flume-ng+Kafka+Storm+HDFS 实时系统组合
1）.数据采集
负责从各节点上实时采集数据，选用cloudera的flume来实现
2）.数据接入
由于采集数据的速度和数据处理的速度不一定同步，因此添加一个消息中间件来作为缓冲，选用apache的kafka
3）.流式计算
对采集到的数据进行实时分析，选用apache的storm
4）.数据输出
对分析后的结果持久化，暂定用mysql
另一方面是模块化之后，假如当Storm挂掉了之后，数据采集和数据接入还是继续在跑着，数据不会丢失，storm起来之后可以继续进行流式计算；


使用Storm实现实时大数据分析实例：用storm来监测车辆速度是否超过80km/h
http://blog.sina.com.cn/s/blog_5ca749810101c0cj.html
基于storm的实时GPS数据客流特征分析系统 源码分析之(一)：GPSReceiverSpout
http://blog.sina.com.cn/s/blog_5ca749810101ceqz.html



flume and mongodb 整合

MongoSink请到这里下载：https://github.com/leonlee/flume-ng-mongodb-sink
MongoSink我简单说一下，只需要把他打成jar包丢到/home/flume/lib里面就行了，当然别忘了把mongodb驱动也丢进去。以后你要是开发其他扩展包都是丢到lib里面就好
好，那么我们就清楚了，flume的作用就是从source获取数据，存入channel缓冲队列，最后由sink放入永久存储

现目前我们主要是解决了日志数据不必直接写mongo库，只要把IP和端口告知其他项目团队，他们直接往这里发送数据就行了。

cd /Applications/apache-flume-1.5.0-bin/
bin/flume-ng agent --conf conf --conf-file conf/flume.conf.mongodb --name agent2 -Dflume.root.logger=INFO,console

启动测试
telnet 127.0.0.1 44444
输入数据
{ "name": "cxh", "sex": "man" }
