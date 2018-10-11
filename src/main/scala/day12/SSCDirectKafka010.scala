package day12
/**
  * spark streaming 整合kafka 0.10版本
  * Created by zhangjingcun on 2018/10/10 16:23.
  */

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SSCDirectKafka010 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local[*]").setAppName(s"${this.getClass.getSimpleName}")
    // 批次时间为2s
    val ssc = new StreamingContext(conf, Seconds(2))

    /**
      * kafka参数列表
      */
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop01:9092,hadoop02:9092,hadoop03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "day12_005",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //指定主题
    val topics = Array("testTopic")

    /**
      * 指定kafka数据源
      */
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    val maped: DStream[(String, String)] = stream.map(record => (record.key, record.value))

    maped.foreachRDD(rdd=>{
      //计算逻辑
      rdd.foreach(println)
    })

    //启动程序
    ssc.start()

    //等待程序被终止
    ssc.awaitTermination()
  }
}
