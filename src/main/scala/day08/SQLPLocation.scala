package day08

import day06.MyUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  *     //Builder 是 SparkSession 的构造器。 通过 Builder, 可以添加各种配置。
  *     getOrCreate:获取或新建一个sparkSession
  */
object SQLPLocation {
  val rulesFilePath = "D:\\data\\spark\\in\\ip.txt"
  val accessFilePath = "D:\\data\\spark\\in\\access.log"
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    //Builder 是 SparkSession 的构造器。 通过 Builder, 可以添加各种配置。
    val spark = SparkSession.builder().appName("SQLPlocation").master("local").getOrCreate()
    //读取IP规则
    val ipRulesLine = spark.read.textFile(rulesFilePath)

    //整理IP规则数据
    import spark.implicits._
    val tpRDDs: Dataset[(Long, Long, String)] = ipRulesLine.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum,endNum,province)
    })
    val ipRulesDF = tpRDDs.toDF("start_Num","end_Num","province")
    //将IP规则数据注册成视图
    ipRulesDF.createTempView("v_ip_rules")
    //读取访问日志数据
    val accessLogLine = spark.read.textFile(accessFilePath)
    val ips: DataFrame = accessLogLine.map(line=> {
      val fields = line.split("[|]")
      val ip = fields(1)
      MyUtils.ip2Long(ip)
    }).toDF("ip")
    //将日志注册成视图
    ips.createTempView("v_access_ip")
    val result = spark.sql("SELECT province,COUNT(*) counts FROM v_ip_rules JOIN v_access_ip ON ip>=start_Num AND ip<=end_Num GROUP BY province ORDER BY counts DESC")

    result.show()
    spark.stop()

  }

}
