package day14
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 将kafka+spark streaming+reids整合词频统计
  * Created by zhangjingcun on 2018/10/12 15:18.
  */
object MyNetworkWordCountRedis {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    //创建StreamingContext对象
    val sparkConf = new SparkConf().setAppName("MyNetworkWordCountRedis").setMaster("local[2]")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "5") //从kafka拉取数据限速，（5）*（分区个数）*（采集数据时间）
    sparkConf.set("spark.streaming.kafka.stopGracefullyOnShutdown", "true") //优雅的停止关闭

    //定义一个采样时间，每隔5秒钟采集一次数据,这个时间不能随意设置
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    //定义一个消费者id
    val groupId = "day14_001"

    //定义一个主题
    val topic = "wordCount"

    /**
      * kafka参数列表
      */
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> "hadoop01:9092,hadoop02:9092,hadoop03:9092", "key.deserializer" -> classOf[StringDeserializer], "value.deserializer" -> classOf[StringDeserializer], "group.id" -> groupId, "auto.offset.reset" -> "earliest", "enable.auto.commit" -> (false: java.lang.Boolean))

    //连接到kafka数据源
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams))

    //1: 创建在Master 节点的Driver
    stream.foreachRDD(rdd => {
      // val reduced: RDD[(ConsumerRecord[String, String], Int)] = rdd.map((_, 1)).reduceByKey(_ + _) //2: 创建在Master 节点的Driver
      val reduced = rdd.map(crd => (crd.value(), 1)).reduceByKey(_+_)
      reduced.foreachPartition(it => {
        //3: Executor（Worker）
        val jedis = JPools.getJedis
        it.foreach({ v =>
          jedis.hincrBy("wordcount", v._1, v._2.toLong)
        })
        jedis.close()
      })
    })

    ssc.start()

    ssc.awaitTermination()
  }
}
