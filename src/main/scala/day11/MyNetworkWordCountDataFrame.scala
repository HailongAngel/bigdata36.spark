package day11

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 实时词频统计集成SparkSql
  * Created by zhangjingcun on 2018/10/9 9:12.
  */
object MyNetworkWordCountDataFrame {
  def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        //创建StreamingContext对象
         val sparkConf = new SparkConf().setAppName("MyNetworkWordCountDataFrame").setMaster("local[2]")

      //定义一个采样时间，每隔2秒钟采集一次数据，这个时间不能随意设置
    val ssc = new StreamingContext(sparkConf,Seconds(5))

     //创建一个离散流，DStream代表输入的数据流
    val lines = ssc.socketTextStream("hadoop01",1234)

    //处理数据
    val words = lines.flatMap(_.split(" "))
    /***
      * 输入：
      *     hadoop  spark hadoop hadoop
      * 输出：
      *      hadoop
      *      spark
      *      hadoop
      *      hadoop
      *
      */

    //使用Spark Sql来查询Spark Streaming的流式数据
    words.foreachRDD(rdd => {
      //创建一个Spark Session对象
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()

      //将Rdd转换成DataFrame
      import  spark.implicits._
      //words是列的名字，表只有一列
      val wordsDataFrame = rdd.toDF("words")

      //创建临时视图
      wordsDataFrame.createOrReplaceTempView("tbl_words")

      //执行Sql， 进行词频统计
      val result = spark.sql("select words, count(*) as total from tbl_words group by words")
      result.show()
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
