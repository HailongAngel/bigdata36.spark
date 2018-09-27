package caseTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 需求四：获取访问次数最多的前N个资源=》TopN
  */
object Need4 {
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
    //返回资源
    val endpoint: RDD[String] = logs.map(_.endpoint)
    //求得访问资源的次数
    val endpointCount = endpoint.map((_,1)).reduceByKey(_+_).sortBy(-_._2)
    //获取访问次数最多的前N个资源=》TopN
    val topN: Array[(String, Int)] = endpointCount.take(N)
    println(s"前${N}个资源分别是${topN.toBuffer}")


  }
}
