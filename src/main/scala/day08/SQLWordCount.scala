package day08
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
object SQLWordCount {
  def main(args: Array[String]): Unit = {
    val filePath = "D:\\data\\in\\index\\b.txt"
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    //在Spark 2.x Sql的编程API的入口是Spark Serssion
    val spark: SparkSession = SparkSession.builder().appName("SQLWordCount").master("local[*]").getOrCreate()

    //指定以后从哪里读取数据，DataSet返回的是有一列的DataFrame
    val lines: Dataset[String] = spark.read.textFile(filePath)

    //整理数据，切分压平
    //DataSet调用RDD上的方法，必须导入隐式转换
    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))
    import spark.implicits._
    words.createTempView("w_words")
    val result: DataFrame = spark.sql("select value word,Count(*) counts from w_words GROUP BY word ORDER BY counts DESC")
    result.show()
    spark.stop()



  }

}
