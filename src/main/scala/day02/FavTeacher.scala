package day02

import java.net.URL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 取得最受欢迎的老师前三名
  *   ((bigdata, wangwu),10)
  *   ((javaee,laoyang),8)
  *
  *   数据：
  *     http://bigdata.edu360.cn/wangwu
  *     http://bigdata.edu360.cn/wangwu
  *     http://javaee.edu360.cn/zhaoliu
  *     http://javaee.edu360.cn/zhaoliu
  *     ......
  * Created by zhangjingcun on 2018/9/18 16:24.
  */
object FavTeacher {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val conf = new SparkConf()
    conf.setAppName("FavTeacher").setMaster("local[2]") //local[*]表示用多个线程跑，2表示用两个线程
    val sc = new SparkContext(conf)

    //读取数据
    val lines: RDD[String] = sc.textFile("D:\\data\\teacher.log")

    //整理数据，每个老师记一次数
    val subjectAddTeacher: RDD[((String, String), Int)] = lines.map(line => {
      val teacher = line.substring(line.lastIndexOf("/") + 1)
      val url = new URL(line).getHost
      val subject = url.substring(0, url.indexOf("."))
      ((subject, teacher), 1)
    })

    //聚合
    val reduced: RDD[((String, String), Int)] = subjectAddTeacher.reduceByKey(_+_)

    //排序
    val sorted = reduced.sortBy(_._2, false)

    //取前三名
    val topN = sorted.take(3)

    println(topN.toBuffer)
  }
}