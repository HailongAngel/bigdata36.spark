package day06

/**
  * 自定义排序
  * 直接在元组内部进行排序
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by zhangjingcun on 2018/9/27 17:41.
  */
object CustomSort4 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("IPLocation").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //用spark对数据进行排序，首先按照颜值的从高到低进行排序，如果颜值相等，在根据年龄的升序排序
    val users: Array[String] = Array("1,tom,99,34", "2,marry,96,26", "3,mike,98,29", "4,jim,96,30")

    //并行化成RDD
    val userLines: RDD[String] = sc.makeRDD(users)

    //整理数据
    val tpRdd: RDD[(Long, String, Int, Int)] = userLines.map(line => {
      val fileds = line.split(",")
      val id = fileds(0).toLong
      val name = fileds(1)
      val fv = fileds(2).toInt
      val age = fileds(3).toInt
      (id, name, fv, age)
    })

    //利用元祖的比较特点：先比较第一个，如果不相等，按照第一个属性排序，在比较下个属性
    implicit val rules = Ordering[(Int, Int)].on[(Long, String, Int, Int)](t => (-t._3, t._4))

    val sorted = tpRdd.sortBy(t => t)
    //收集数据
    val result: Array[(Long, String, Int, Int)] = sorted.collect()

    println(result.toBuffer)

    sc.stop()
  }
}
