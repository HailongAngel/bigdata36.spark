package day13

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}

/**
  * spark streaming 整合kafka 0.10版本,输出分区、主题等信息
  * Created by zhangjingcun on 2018/10/11 8:28.
  */
object SSCDirectKafka010 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local[*]").setAppName(s"${this.getClass.getSimpleName}")
    // 批次时间为2s
    val ssc = new StreamingContext(conf, Seconds(2))

    val groupId = "day12_007";

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

    //指定主题
    val topics = Array("testTopic")

    /**
      *   指定kafka数据源
      *   ssc：StreamingContext的实例
      *   LocationStrategies：位置策略，如果kafka的broker节点跟Executor在同一台机器上给一种策略，不在一台机器上给另外一种策略
      *       设定策略后会以最优的策略进行获取数据
      *       一般在企业中kafka节点跟Executor不会放到一台机器的，原因是kakfa是消息存储的，Executor用来做消息的计算，
      *       因此计算与存储分开，存储对磁盘要求高，计算对内存、CPU要求高
      *       如果Executor节点跟Broker节点在一起的话使用PreferBrokers策略，如果不在一起的话使用PreferConsistent策略
      *       使用PreferConsistent策略的话，将来在kafka中拉取了数据以后尽量将数据分散到所有的Executor上
      *   ConsumerStrategies：消费者策略（指定如何消费）
      *
      */
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD(rdd=>{
      val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val maped: RDD[(String, String)] = rdd.map(record => (record.key, record.value))
      //计算逻辑
      maped.foreach(println)

      //循环输出
      for(o<-offsetRange){
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }

    })

    //启动程序
    ssc.start()

    //等待程序被终止
    ssc.awaitTermination()
  }
}
