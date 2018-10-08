package day10
/**
  * 开发自己的实时词频统计程序(累计单词出现次数)
  * 可以累积之前的结果，也可以在程序结束之后加载检查点所保存的信息，从而接住之前的数据
  * Created by zhangjingcun on 2018/10/8 14:19.
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyNetworkTotalWordCountV2 {
  val ckp = "./ckp"

  /**
    * 该函数会作用在相同key的value上
    */
  //
  def updateFunction (newValues:Seq[Int],runningCount:Option[Int]):Option[Int] = {
    //得到当前的总和
    val currentTotal = newValues.sum
    //执行累加操作，如果是第一次执行（如果单词第一次执行，则没有之前的值）
    val totalValues = runningCount.getOrElse(0)
    Some(currentTotal+totalValues)
  }
  //如果从checkout目录中恢复不了上一个Job的实例，则创建一个新的ssc
  def funcToCreateContext() = {
    println("new Create")
    //创建StreamingContext对象
    val sparkConf = new SparkConf().setAppName("MyNetworkWordCount").setMaster("local[2]")
    //定义一个采样时间，每隔2秒采集一次数据，这个时间不能随意设置
    val ssc = new StreamingContext(sparkConf,Seconds(2))
    //设置检查点目录
    ssc.checkpoint(ckp)
    //创建一个离散流，DStream代表输入的数据流
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop01", 1122)
    //设置checkpoint，默认每6秒做一次checkpoint
    lines.checkpoint(Seconds(6))
    //处理分词，每个单词记一次数
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(x=>(x, 1))

    //累加
    val totalResult = pairs.updateStateByKey(updateFunction _)
    totalResult.print()

    ssc
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    //定义一个采样时间，每隔2秒钟采集一次数据,这个时间不能随意设置
    val ssc = StreamingContext.getOrCreate(ckp, funcToCreateContext _)

    //开始计算
    ssc.start()

    //等待计算被中断
    ssc.awaitTermination()
  }
}
