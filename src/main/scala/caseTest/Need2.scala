package caseTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  *
  * 需求二：统计各个不同返回值出现的数据个数（WordCount）
  */
object Need2 {
  def main(args: Array[String]): Unit = {
    //在本地运行
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    //加载Spark
    val conf = new SparkConf()
    conf.setAppName("Need1").setMaster("local")
    val sc = new SparkContext(conf)
    //加载数据
    val lines: RDD[String] = sc.textFile("D:\\data\\in\\access_2013_05_31.log")
    //清理数据
    val logs = lines.filter(line=>ApacheAccessLog.isValidateLogLine(line)).map(line => {
      val log = ApacheAccessLog.parseLogLine(line)
      log
    })
     //取得返回值
    val response = logs.map(log => log.responseCode)
    //进行分组聚合
    val responseCount = response.map((_,1)).reduceByKey(_+_)
    println(s"个数为${responseCount.collect().toBuffer}")


  }
}
