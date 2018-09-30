package day08

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * spark sql 2.x版本demo
  * Created by zhangjingcun on 2018/9/29 14:21.
  */
object SQLDemo2x1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    //
    //    //spark rdd程序执行的入口是SparkContext
    //    val conf = new SparkConf().setAppName("SQLDemo1x1").setMaster("local[*]")
    //    val sc = new SparkContext(config = conf)
    //
    //    //SQLContext是对SparkContext的一个包装（增强了功能，可以处理结构化数据）
    //    val sqlContext = new SQLContext(sc)

    //在Spark 2.x Sql的编程API的入口是Spark Serssion
    val session: SparkSession = SparkSession.builder()
      .appName("SQLDemo2x1")
      .master("local[*]").getOrCreate()

    //DataFrame = RDD + Schema
    //将RDD添加额外的信息，编程DF
    val lines: RDD[String] = session.sparkContext.parallelize(List("1,tom,99,29", "2,marry,98,30", "3,jim,98,27"))

    //整理数据，将RDD管理schema信息
    val rowRDD: RDD[Row] = lines.map(line=>{
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val fcValue = fields(2).toDouble
      val age = fields(3).toInt

      Row(id, name, fcValue, age)
    })

    //有Rdd了，但是没有schema信息（元数据），构造一个schema
    val schema = StructType(
      List(
        StructField("id", LongType),
        StructField("name", StringType),
        StructField("fv", DoubleType),
        StructField("age", IntegerType)
      )
    )

    val df: DataFrame = session.createDataFrame(rowRDD, schema)

    //对df进行处理，注册表（视图）
    df.createTempView("student")

    val result = session.sql("SELECT name, fv, age FROM student ORDER BY fv DESC, age ASC")

    //执行Action
    result.show()

    //    result.foreachPartition(it =>{
    //
    //    })

    //释放资源
    session.stop()
  }

}
