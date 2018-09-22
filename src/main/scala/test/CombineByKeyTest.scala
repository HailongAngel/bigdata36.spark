package test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object CombineByKeyTest {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val conf = new SparkConf()
    conf.setAppName("FavTeacher").setMaster("local[2]") //local[*]表示用多个线程跑，2表示用两个线程
    val sc = new SparkContext(conf)
    val pairRDD = sc.parallelize(List(("hello",2),("jerry",3),("hello",4),("jerry",1)),2)
    //第一个分函数的功能：将分好组的key的第一个value取出来进行操作
    //第二个函数的功能：在每个区中，将value的其他元素加入进来
    //第三个函数的功能，将不同区中的相同的key的value加进来
    val rdd2 = pairRDD.combineByKey(x=>x,(m:Int,n:Int)=>m+n,(a:Int,b:Int)=>a+b)
    println(rdd2.collect().toBuffer)
  }

}
