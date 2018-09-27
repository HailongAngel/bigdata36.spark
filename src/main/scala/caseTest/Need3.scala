package caseTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 需求三：获取访问次数超过N次的IP地址
  */
object Need3 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    //加载Spark
    val conf = new SparkConf()
    val N = args(0).toInt
    conf.setAppName("Need1").setMaster("local")
    val sc = new SparkContext(conf)
    //加载数据
    val lines: RDD[String] = sc.textFile("D:\\data\\in\\access_2013_05_31.log")
    //清理数据
    val logs = lines.filter(line=>ApacheAccessLog.isValidateLogLine(line)).map(line => {
      val log = ApacheAccessLog.parseLogLine(line)
      log
    })
    //返回IP地址
    val IP = logs.map(ip => ip.ipAddress)
    //将IP地址的出现次数相加
    val tokNCount = IP.map((_,1)).reduceByKey(_+_)
    //取得大于N的IP地址
    val latter = tokNCount.filter(_._2>N)
    println(s"大于N次的IP地址数据为${latter.collect().toBuffer}")
  }

}
