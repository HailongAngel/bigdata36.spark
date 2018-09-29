package day08

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

object SQLDemo1x2 {
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
    var studentRDD = lines.map(line=>{
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val fv = fields(2).toDouble
      val age = fields(3).toInt
      Row(id,name,fv,age)
    })
    //有Rdd了，但是没有schema信息（元数据），构造一个schema
    val scheme = StructType(
      List(
        StructField("id",LongType),
        StructField("name",StringType),
        StructField("fv",DoubleType),
        StructField("age",IntegerType)

      )
    )
    //RDD关联schema
    val df = sqlContext.createDataFrame(studentRDD,scheme)
    //不用sql风格的语法，使用DSL风格的语法，即调用DataFrame的方法
    //select方法也是一个transformastion
    val selected = df.select("name","fv","age")
    //排序
    //导入隐式转换，使用lamdba表达式
    import sqlContext.implicits._
    val result = selected.orderBy($"fv"desc,$"age"asc)
    //触发Action
    result.show()
    //释放资源
    sc.stop()


}
}
