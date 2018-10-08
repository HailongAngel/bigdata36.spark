package day10
/**
  * 开发自己的实时词频统计程序(累计单词出现次数)
  * 可以累积之前的统计，但是一关闭程序之前的记录就清空了
  * Created by zhangjingcun on 2018/10/8 14:19.
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object MyNetworkTotalWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName("MyNetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    ssc.checkpoint("./skt")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop01",5678)

    val result: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1))

    val updateStateFunc = (currValues:Seq[Int], preValues:Option[Int])=>{
      val currentTotal = currValues.sum
      val totalValues = preValues.getOrElse(0)
      Some(currentTotal+totalValues)
    }
    val totalResult = result.updateStateByKey(updateStateFunc)

    //输出
    totalResult.print()

    //启动StreamingContext
    ssc.start()

    //等待计算完成
    ssc.awaitTermination()


}
}
