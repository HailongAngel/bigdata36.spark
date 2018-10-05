package day09

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
/**
  * DateSet接收jdbc数据源，并且进行相关操作
  * 还可以将数据保存成多种格式
  */
object JdbcDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JdbcDataSource")
      .master("local")
       .getOrCreate()
    import spark.implicits._
    val logs: DataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://localhost:3306/bigdata",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "logs",
        "user" -> "root",
        "password" -> "926718")
    ).load()

   /* //使用函数的方式
    val filtered = logs.filter( r =>{
      r.getAs[Int]("age") <13
    }
    )
    filtered.show()*/
    //lambda表达式
    val r = logs.filter($"age"<=13)
    val result = r.select($"id",$"name",$"age" * 10 as "new_age")
    result.show()

    //将数据写入数据库
    val props = new Properties()
    props.put("user","root")
    props.put("password","926718")
    //result.write.mode("ignore").jdbc("jdbc:mysql://localhost:3306/bigdata", "logs1", props)
    //有的已经存在可以mode = overwrite
    //DataFrame保留了元组信息，所以知道有多少列
    result.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/bigdata", "logs1", props)


    //将数据保存成text时出错（只能保存一列,因为保存数据的同时还需要保存元数据信息，但是文本没有这个功能）
  //  result.write.text("D:\\data\\spark\\in\\text")
    //将数据保存成json格式
   // result.write.json("D:\\data\\spark\\in\\json")
    result.write.csv("D:\\data\\spark\\in\\csv")
    result.write.parquet("D:\\data\\spark\\in\\parquet")
  }

}
