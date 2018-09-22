package day03


/**
  * 根據學科取得最受欢迎的老师前2名（过滤后排序）
  *   ((bigdata, wangwu),10)
  *   ((javaee,laoyang),8)
  *
  *   数据：
  *     http://bigdata.edu360.cn/wangwu
  *     http://bigdata.edu360.cn/wangwu
  *     http://javaee.edu360.cn/zhaoliu
  *     http://javaee.edu360.cn/zhaoliu
  *     ......
  */
import java.net.URL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavTeacherWithObject2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf()
    conf.setAppName("FavTeacherWithObject2").setMaster("local")
    val subjects = Array("bigdata", "javaee", "php")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D:\\data\\in\\teacher\\teacher.log")
    //整理数据
    val sbjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })

    //和一组合在一起(不好，调用了两次map方法)
    //val map: RDD[((String, String), Int)] = sbjectAndteacher.map((_, 1))

    //聚合，将学科和老师联合当做key
    val reduced: RDD[((String, String), Int)] = sbjectTeacherAndOne.reduceByKey(_+_)

    //cache到内存
    //val cached = reduced.cache()

    //scala的集合排序是在内存中进行的，但是内存有可能不够用
    //可以调用RDD的sortby方法，内存+磁盘进行排序

    for (sb <- subjects) {
      //该RDD中对应的数据仅有一个学科的数据（因为过滤过了）
      val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1 == sb)

      //现在调用的是RDD的sortBy方法，(take是一个action，会触发任务提交)
      val favTeacher = filtered.sortBy(_._2, false).take(2)

      //打印
      println(favTeacher.toBuffer)
    }

    sc.stop()


  }
}
