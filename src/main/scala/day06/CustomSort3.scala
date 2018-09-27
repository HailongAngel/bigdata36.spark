package day06

/**
  * 自定义排序
  *对象里面返回元组的方式
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by zhangjingcun on 2018/9/27 17:37.
  */
object CustomSort3 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("IPLocation").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //用spark对数据进行排序，首先按照颜值的从高到低进行排序，如果颜值相等，在根据年龄的升序排序
    val users: Array[String] = Array("1,tom,99,34", "2,marry,96,26", "3,mike,98,29", "4,jim,96,30")

    //并行化成RDD
    val userLines: RDD[String] = sc.makeRDD(users)

    //整理数据
    val userRdd: RDD[(Long, String, Int, Int)] = userLines.map(line => {
      val fileds = line.split(",")
      val id = fileds(0).toLong
      val name = fileds(1)
      val fv = fileds(2).toInt
      val age = fileds(3).toInt
      (id, name, fv, age)
    })

    //排序
    val sorted: RDD[(Long, String, Int, Int)] = userRdd.sortBy(tp => User3(tp._1, tp._2, tp._3, tp._4))

    //收集数据
    val result: Array[(Long, String, Int, Int)] = sorted.collect()

    println(result.toBuffer)

    sc.stop()
  }
}
//case 可以不使用new关键字
//不需要实现序列化
case class User3 (val id:Long, val name:String, val fv:Int, val age:Int) extends Ordered[User3] {
  override def compare(that: User3): Int = {
    //颜值相等的时候
    if (that.fv == this.fv) {
      this.age - that.age
    } else {
      -(this.fv - that.fv)
    }
  }

  override def toString: String = {
    s"User:($id, $name, $fv, $age)"
  }
}

