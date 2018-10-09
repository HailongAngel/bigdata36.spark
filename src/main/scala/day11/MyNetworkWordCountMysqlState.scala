package day11
import java.sql.DriverManager

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
/**
  * 将实时词频统计的数据写入到mysql数据库（基于State统计）
  * Created by zhangjingcun on 2018/10/9 9:43.
  */
object MyNetworkWordCountMysqlState {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    //会去加载resources下面的配置文件，默认规则：application.conf->application.json->application.properties
    val config = ConfigFactory.load()

    //创建StreamingContext对象
    val sparkConf = new SparkConf().setAppName("MyNetworkWordCountMysqlState").setMaster("local[2]")

    //定义一个采样时间，每隔5秒钟采集一次数据,这个时间不能随意设置
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //创建一个离散流，DStream代表输入的数据流
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop01", 5678)

    /**
      * 插入当前批次计算出来的数据结果
      * foreachRDD 在Driver端运行
      * foreachPartition、foreach在worker端运行
      */
    lines.foreachRDD(rdd => {
      //计算当前批次结果
      val curr_batch_result: RDD[(String, Int)] = rdd.flatMap(line => line.split(" ")).filter(!_.isEmpty).map(word => (word, 1)).reduceByKey(_ + _)

      //插入当前批次计算出来的数据结果
      curr_batch_result.foreachPartition(partition => {
        //创建一个连接
        val url = config.getString("db.url")
        val user = config.getString("db.user")
        val password = config.getString("db.password")
        val conn = DriverManager.getConnection(url, user, password)

        //将当前分区里面的所有数据都插入到mysql数据库中
        partition.foreach(tp => {
          val word = tp._1
          //判断即将插入的数据是否之前已经插入过，如果已经插入过，则进行更新操作，否则就是插入
          val pstmts = conn.prepareStatement("select * from wordcount where words=?")
          pstmts.setString(1, word)
          val rs = pstmts.executeQuery()
          var flag = false
          while (rs.next()) {
            println(s"${word}数据库中存在")
            flag = true  //已经存在该单词
            //即将插入的单词已经存在，可以进行更新操作
            val dbCurrCount = rs.getInt("total")
            //计算最新的值
            val newCount = dbCurrCount + tp._2
            //更新
            val update = conn.prepareStatement("update wordcount set total=? where words=?")
            update.setInt(1, newCount)
            update.setString(2, word)
            update.executeUpdate()
            update.close()
          }
          rs.close()
          pstmts.close()

          if (!flag){
            //插入一条数据
            val stmts = conn.prepareStatement("insert into wordcount values(?,?)")
            stmts.setString(1, tp._1)
            stmts.setInt(2, tp._2)
            stmts.executeUpdate()
            pstmts.close()
          }
        })
        if (conn!=null) conn.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
