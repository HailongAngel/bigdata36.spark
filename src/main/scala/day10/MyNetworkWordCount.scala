package day10

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * 开发自己的实时词频统计程序
  * 但是不能累加，每一次都是新的统计次数
  * Created by zhangjingcun on 2018/10/8 10:17.
  */
object MyNetworkWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    //创建StreamingContext对象
    val SparkConf = new SparkConf().setAppName("MyNetworkWordCount").setMaster("local[2]")
    //定义一个采样时间，每隔两秒钟采集一次数据
    val ssc = new StreamingContext(SparkConf,Seconds(2))
    //创建一个离散流，DStream代表输入的数据流
    val lines = ssc.socketTextStream("hadoop01",5678)
    //处理数据
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val result = words.map(x=>(x,1)).reduceByKey(_+_)
    //输出结果
    result.print()
    //启动StreamingContext,开始执行计算
    ssc.start()
    //等待计算完成
    ssc.awaitTermination()


  }
}
