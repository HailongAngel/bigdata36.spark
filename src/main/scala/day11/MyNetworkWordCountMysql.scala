package day11

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * 将实时词频统计的数据写入到mysql数据库
  * Created by zhangjingcun on 2018/10/9 9:43.
  */
object MyNetworkWordCountMysql {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    //会去加载resources下面的配置文件，默认规则：application.conf->application.json->application.properties
    val config = ConfigFactory.load()

    //创建StreamingContext对象
    val sparkConf = new SparkConf().setAppName("MyNetworkWordCount").setMaster("local[2]")

    //定义一个采样时间，每隔2秒钟采集一次数据,这个时间不能随意设置
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //创建一个离散流，DStream代表输入的数据流
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop01", 5678)

    //插入当前批次计算出来的数据结果
    lines.foreachRDD(rdd => {
      //创建一个Spark Session对象
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()

      //将Rdd转换成DataFrame
      import spark.implicits._
      //words是列的名字，表只有一列
      val wordsDataFrame = rdd.flatMap(line => line.split(" ")).toDF("words")

      //创建临时视图
      wordsDataFrame.createOrReplaceTempView("wordcount")

      //执行Sql， 进行词频统计
      val result = spark.sql("select words, count(*) as total from wordcount group by words")

      //封装用户名和口令
      val props = new Properties()
      props.setProperty("user", config.getString("db.user"))
      props.setProperty("password", config.getString("db.password"))

      if (!result.rdd.isEmpty()) {
        result.write.mode(SaveMode.Append).jdbc(config.getString("db.url"), config.getString("db.table"), props)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
