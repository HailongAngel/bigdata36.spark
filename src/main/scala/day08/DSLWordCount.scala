package day08
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, SparkSession}

/**
  * DSL语句实现WordCount
  * Created by zhangjingcun on 2018/9/29 15:59.
  */
object DSLWordCount {
  val filePath = "D:\\data\\1.txt"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    //在Spark 2.x Sql的编程API的入口是Spark Serssion
    val spark = SparkSession.builder().appName("SQLWordCount").master("local[*]").getOrCreate()

    //指定以后从哪里读取数据，DataSet返回的是有一列的DataFrame
    val lines: Dataset[String] = spark.read.textFile(filePath)

    //整理数据，切分压平
    //DataSet调用RDD上的方法，必须导入隐式转换
    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))

    //分组计算
    val grouped: RelationalGroupedDataset = words.groupBy($"value" as "word")

    //transfotmation
    val counted: DataFrame = grouped.count()

    //Action
    counted.show()

    spark.stop()
  }
}
