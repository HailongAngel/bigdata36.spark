package day02

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

object FavTeacher1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FavTeacher1").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D:\\data\\teacher.log")
    val word = lines.map(line=>{
      val teacher = line.substring(line.lastIndexOf("/"+1))
      val url = new URL(line).getHost
      val subject = url.substring(0,url.indexOf("."))
    })

  }
}
