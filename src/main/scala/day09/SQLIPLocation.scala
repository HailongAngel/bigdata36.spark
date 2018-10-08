package day09

import day06.MyUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.SparkSession

/**
  * 使用广播变量结合Spark Sql实现Ip地理位置匹配
  * Created by zhangjingcun on 2018/9/30 8:21.
  */
object SQLIPLocation {
  val rulesFilePath = "D:\\data\\spark\\in\\ip.txt"
  val accessFilePath = "D:\\data\\spark\\in\\access.log"
  def main(args: Array[String]): Unit = {
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val spark: SparkSession = SparkSession.builder().appName("SQLIPLocation").master("local").getOrCreate()
    //读取IP资源库
    val ipRulesLines = spark.read.textFile(rulesFilePath)
    //整理IP规则
    import spark.implicits._
    val ipRules = ipRulesLines.map(line =>{
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    })
    //将全部的IP收集到Driver端
    val ipRulesInDriver = ipRules.collect()
    //将收集到的IP广播，广播是阻塞的方法，如果没有广播完就不会往下执行（广播变量的引用在Driver端）
    val broadcastRef = spark.sparkContext.broadcast(ipRulesInDriver)
    //读取访问日志数据
    val accessLogLines = spark.read.textFile(accessFilePath)
    //整理访问日志数据
    val ips = accessLogLines.map(line=>{
      val fields = line.split("[|]")
      val ip = fields(1)
      MyUtils.ip2Long(ip)
    }).toDF("ip_num")
    //将访问日志注册成视图
    ips.createTempView("v_access_ip")
    //定义并注册自定义函数
    //自定义函数是在哪里定义的？（Driver）,业务逻辑是在哪里执行的？（Executor）
    spark.udf.register("ip_num2Province",(ipNum:Long)=>{
      //获取广播到Executor端的全部IP规则
      //根据Driver端的广播变量的引用，在发送Task时，会将Driver端的引用伴随着发送到Exector
      val rulesInExecutor = broadcastRef.value
      val index = MyUtils.binarySearch(rulesInExecutor, ipNum)
      var province = "未知"
      if (index != -1){
        province = rulesInExecutor(index)._3
      }
      province
    })
   //写SQL，是一个transformation
    val result = spark.sql("select ip_num2Province(ip_num) province,count(*) counts from v_access_ip GROUP BY province ORDER BY counts DESC")

    //输出结果
    result.show()
    //释放资源
    spark.stop()

  }
}
