package test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ExpertOperator {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val conf = new SparkConf()
    conf.setAppName("FavTeacher").setMaster("local[2]") //local[*]表示用多个线程跑，2表示用两个线程
    val sc = new SparkContext(conf)
    println("------------------------collectMap------------------------------------------")
    //初始化一个链表
    val rdd: RDD[(String, Int)] = sc.parallelize(List(("a",1),("b",2),("b",32)))
    //转换为Map
    val rdd1: collection.Map[String, Int] = rdd.collectAsMap
    println(rdd.collect().toBuffer)
    println(rdd1.toBuffer)
    //从结果我们可以看出，如果RDD中同一个Key中存在多个Value，那么后面的Value将会把前面的Value覆盖，最终得到的结果就是Key唯一，而且对应一个Value。
    println("-------------------------countByKey-----------------------------------------")
    val rdd01 = sc.parallelize(List(("a",1),("b",2),("b", 32), ("b", 12), ("c", 11)))
    //返回key的个数
    val rdd02 = rdd01.countByKey
    println(rdd02.toBuffer)
    println("-------------------------countByValue-----------------------------------------")
    val rdd11 =  sc.parallelize(List(("a", 1), ("b", 2), ("b",2),("b", 32), ("b", 12), ("c", 11)))
    //统计相同的key+value出现的次数
    val rdd12 = rdd11.countByValue()
    println(rdd12.toBuffer)
    println("-------------------------flatMapValues-----------------------------------------")
    val rdd21 =  sc.parallelize(List(("a", "1 2"), ("b", "3 4")))
    //同基本转换操作中的flatMap，只不过flatMapValues是针对[K,V]中的V值进行flatMap操作。
    val rdd22 = rdd21.flatMapValues(_.split(" "))
    println(rdd22.collect().toBuffer)
  }
}
