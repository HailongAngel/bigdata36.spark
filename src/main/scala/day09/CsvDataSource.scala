package day09

import org.apache.spark.sql.SparkSession

//读取CSV数据格式的文件
object CsvDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CsvDataSource")
      .master("local")
      .getOrCreate()

    val csv =  spark.read.csv("D:\\data\\spark\\in\\csv\\part-00000-bb725b79-020e-4020-bac3-be05dc1828bc-c000.csv")
    //因为自定义的表头信息都是没有含义的，所以自己取一些比较有意义的作为表头
    val pdf = csv.toDF("id","name","age")
    pdf.show()
    spark.stop()
  }
}
