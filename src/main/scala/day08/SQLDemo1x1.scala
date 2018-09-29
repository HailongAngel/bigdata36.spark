package day08
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/** 这是一个sqark sql 1.x版本的demo写法
  * Created by zhangjingcun on 2018/9/29 9:40.
  */
object SQLDemo1x1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    //spark rdd程序执行的入口是SparkContext
    val conf = new SparkConf().setAppName("SQLDemo1x1").setMaster("local[*]")
    val sc = new SparkContext(config = conf)

    //SQLContext是对SparkContext的一个包装（增强了功能，可以处理结构化数据）
    val sqlContext  = new SQLContext(sc)

    //DataFrame = RDD + Schema
    //将RDD添加额外的信息，编程DF
    val lines: RDD[String] =  sc.parallelize(List("1,tom,99,29", "2,marry,98,30", "3,jim,98,27"))

    //整理数据，将RDD管理scheme信息
    var studentRDD: RDD[Student1] = lines.map(line=>{
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val fv = fields(2).toDouble
      val age = fields(3).toInt

      Student1(id, name, fv, age)
    })

    //将RDD转换成DataFrame
    //导入隐式转换

    import sqlContext.implicits._
    val df: DataFrame = studentRDD.toDF

    //对DataFrame进行操作
    //使用Sql风格的API
    df.registerTempTable("student")

    //这时候不执行sql，因为sql是一个Tranformation
    val result: DataFrame = sqlContext.sql("SELECT name, fv, age FROM student ORDER BY fv desc, age asc")

    //触发action
    result.show()

    //释放资源
    sc.stop()
  }
}

//定义一个caseclass，将数据保存到case class
//case class 的特点：不用new， 实现序列化，模式匹配
case class Student1(id:Long, name:String, fv:Double, age:Int)