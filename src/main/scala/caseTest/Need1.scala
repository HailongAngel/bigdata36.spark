package caseTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex
/**
  * 需求一：求contentSize的平均值、最小值、最大值
  * 64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] "GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1" 401 12846
  * Created by ibf on 01/15.
  */
object Need1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf()
    conf.setAppName("Need1").setMaster("local")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("D:\\data\\in\\access_2013_05_31.log")
    val logs = lines.filter(line=>ApacheAccessLog.isValidateLogLine(line)).map(line => {
      val log = ApacheAccessLog.parseLogLine(line)
      log
    })
    val contentSize = logs.map(log => log.contentSize)
    //求平均值
   val ave =  contentSize.sum()/contentSize.count()
    //求最大值
    val max = contentSize.max()
    //求最小值
    val  min = contentSize.min()
    println(s"平均值为：${ave}，最大值为${max},最小值为${min}")
    sc.stop()
  }
}
