package day09

import org.apache.spark.sql.SparkSession
//读取json文件
object JsonDataSource {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JsonDataSource")
      .master("local")
      .getOrCreate()
    import  spark.implicits._
    val jsons = spark.read.json("JSON的路径")
    val filtered = jsons.filter($"age" <= 500)
    filtered.printSchema()
    spark.stop()

  }
}
