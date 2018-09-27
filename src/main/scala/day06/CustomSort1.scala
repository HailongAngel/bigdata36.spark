package day06

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 自定义排序
  */
object CustomSort1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("IPLocation").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //用spark对数据进行排序，首先按照颜值的从高到低进行排序，如果颜值相等，在根据年龄的升序排序
    val users: Array[String] = Array("1,tom,99,34", "2,marry,96,26", "3,mike,98,29", "4,jim,96,30")

    //并行化成RDD
    val userLines: RDD[String] = sc.makeRDD(users)

    //整理数据
   val userRdd =  userLines.map(line =>{
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val fv = fields(2).toInt
      val age = fields(3).toInt
      new User1(id,name,fv,age)
    })
    //排序
   val sorted =  userRdd.sortBy(u => u)
    //收集数据
    val result = sorted.collect()
    println(result.toBuffer)
    sc.stop()
  }
}
class User1 (val id:Long, val name:String, val fv:Int, val age:Int) extends Ordered[User1] with Serializable{
  override def compare(that: User1): Int = {
    //颜值相等的时候
    if (that.fv == this.fv){
      this.age - that.age
    }else{
      -(this.fv-that.fv)
    }
  }
  override def toString: String = {
    s"User:($id,$name,$fv,$age)"
  }
}