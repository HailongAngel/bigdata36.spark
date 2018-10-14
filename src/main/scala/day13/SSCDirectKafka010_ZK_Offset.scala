package day13

import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * zk管理kafka的offset,输出分区等信息
  * Created by zhangjingcun on 2018/10/11 8:49.
  */
object SSCDirectKafka010_ZK_Offset {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local[*]").setAppName(s"${this.getClass.getSimpleName}")
    // 批次时间为2s
    val ssc = new StreamingContext(conf, Seconds(2))

    val groupId = "day13_001";

    /**
      * kafka参数列表
      */
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop01:9092,hadoop02:9092,hadoop03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topic = "testTopic"
    val topics = Array(topic)

    /**
      * 如果我们自己维护偏移量
      * 问题：
      *   1：程序在第一次启动的时候，应该从什么开始消费数据？earliest
      *   2：程序如果不是第一次启动的话，应该 从什么位置开始消费数据？上一次自己维护的偏移量接着往后消费，比如上一次存储的offset=88
      *
      * 该类主要拼接字符串
      */
    val zKGroupTopicDirs: ZKGroupTopicDirs = new ZKGroupTopicDirs(groupId, topic)

    /**
      * 生成的目录结构
      * /customer/day13_001/offsets/testTopic
      */
    val offsetDir = zKGroupTopicDirs.consumerOffsetDir

    //zk字符串连接组
    val zkGroups = "hadoop01:2181,hadoop02:2181,hadoop03:2181"

    /**
      * 创建一个zkClient连接
      * 判断/customer/day13_001/offsets/testTopic 下面有没有孩子节点，如果有说明之前维护过偏移量，如果没有的话说明程序是第一次执行
      */
    val zkClient = new ZkClient(zkGroups)
    val childrenCount = zkClient.countChildren(offsetDir)

    val stream =  if(childrenCount>0){ //非第一次启动
      println("----------已经启动过------------")
      //用来存储我们读取到的偏移量
      var fromOffsets = Map[TopicPartition, Long]()
      //customer/day13_001/offsets/testTopic/0
      //customer/day13_001/offsets/testTopic/1
      //customer/day13_001/offsets/testTopic/2
      (0  until childrenCount).foreach(partitionId => {
        val offset = zkClient.readData[String](offsetDir+s"/${partitionId}")
        fromOffsets += (new TopicPartition(topic, partitionId) -> offset.toLong)
      })
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets))
    } else { //第一次启动
      println("-------------第一次启动-----------")
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
    }

    stream.foreachRDD(rdd=>{
      val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val maped: RDD[(String, String)] = rdd.map(record => (record.key, record.value))
      //计算逻辑
      maped.foreach(println )

      //自己存储数据，自己管理
      for(o<-offsetRange){
        //println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        //写入到Zookeeper
        ZkUtils(zkClient, false).updatePersistentPath(offsetDir+"/"+o.partition, o.untilOffset.toString)
      }
    })

    ssc.start()

    ssc.awaitTermination()
  }
}
