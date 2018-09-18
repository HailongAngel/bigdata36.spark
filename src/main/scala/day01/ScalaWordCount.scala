package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 这是一个scala版本的Spark词频统计程序
  * Created by zhangjingcun on 2018/9/17 16:01.
  */
object ScalaWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ScalaWordCount")
    //SparkContext，是Spark程序执行的入口
    val sc = new SparkContext(conf)

    //通过sc指定以后从哪里读取数据
    //RDD弹性分布式数据集，一个神奇的大集合
    val lines: RDD[String] = sc.textFile(args(0))

    //将内容分词后压平
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //将单词和1组合到一起
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))

    //分组聚合
    val reduce: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)

    //排序
    val sorted = reduce.sortBy(_._2, false)

    //保存结果
    sorted.saveAsTextFile(args(1))

    //释放资源
    sc.stop()
  }
}
