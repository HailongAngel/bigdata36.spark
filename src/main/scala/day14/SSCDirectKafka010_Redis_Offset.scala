package day14


import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * redis管理kafka消费数据的偏移量
  * Created by zhangjingcun on 2018/10/12 16:17.
  */
object SSCDirectKafka010_Redis_Offset {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    //创建StreamingContext对象
    val sparkConf = new SparkConf().setAppName("MyNetworkWordCountRedis").setMaster("local[2]")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "5") //从kafka拉取数据限速，（5）*（分区个数）*（采集数据间隔秒数）
    sparkConf.set("spark.streaming.kafka.stopGracefullyOnShutdown", "true") //优雅的停止关闭
    //定义一个采样时间，每隔5秒钟采集一次数据,这个时间不能随意设置
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    //定义一个消费者id
    val groupId = "day14_002"

    //定义一个主题
    val topic = "wordCount"

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

    val formOffset: Map[TopicPartition, Long] = JedisOffSet(groupId)

    //连接到kafka数据源
    val stream = if(formOffset.size == 0) {
      KafkaUtils.createDirectStream(ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams))
    } else {
      KafkaUtils.createDirectStream(ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Assign[String, String](formOffset.keys, kafkaParams, formOffset))
    }

    //1: 创建在Master 节点的Driver
    stream.foreachRDD(rdd => {
      val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // val reduced: RDD[(ConsumerRecord[String, String], Int)] = rdd.map((_, 1)).reduceByKey(_ + _)
      // 2: 创建在Master 节点的Driver
      val reduced = rdd.map(crd => (crd.value(), 1)).reduceByKey(_ + _)
      reduced.foreachPartition(it => {
        //3: Executor（Worker）
        val jedis = JPools.getJedis
        it.foreach({ v =>
          jedis.hincrBy("wordcount", v._1, v._2.toLong)
        })
        jedis.close()
      })

      //将偏移量存入redis
      val jedis = JPools.getJedis
      for (o <- offsetRange) {
        jedis.hset(groupId, o.topic + "-" + o.partition, o.untilOffset.toString)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
