package day03

/**
  * 根據學科取得最受欢迎的老师前2名（自定义分区）
  *   ((bigdata, wangwu),10)
  *   ((javaee,laoyang),8)
  *
  *   数据：
  *     http://bigdata.edu360.cn/wangwu
  *     http://bigdata.edu360.cn/wangwu
  *     http://javaee.edu360.cn/zhaoliu
  *     http://javaee.edu360.cn/zhaoliu
  *     ......
  * Created by zhangjingcun on 2018/9/19 8:36.
  * */
import java.net.URL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
object FavTeacherWithObject04 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val topN = args(0).toInt
    val conf = new SparkConf()
    conf.setAppName("FavTeacher").setMaster("local[2]") //local[*]表示用多个线程跑，2表示用两个线程
    val sc = new SparkContext(conf)

    //读取数据
    val lines: RDD[String] = sc.textFile("D:\\data\\in\\teacher\\teacher.log")

    //整理数据，每个老师记一次数
    val subjectAddTeacher: RDD[((String, String), Int)] = lines.map(line => {
      val teacher = line.substring(line.lastIndexOf("/") + 1)
      val url = new URL(line).getHost
      val subject = url.substring(0, url.indexOf("."))
      ((subject, teacher), 1)
    })


    //计算有多少学科
    val subjects: Array[String] = subjectAddTeacher.map(_._1._1).distinct().collect()

    //自定义一个分区器，并且按照指定的分区器进行分区
    val sbPatitioner = new SubjectParitioner2(subjects);

    //聚合，将学科和老师联合当做key
    val reduced: RDD[((String, String), Int)] = subjectAddTeacher.reduceByKey(sbPatitioner,_+_)
    //partitionBy按照指定的分区规则进行分区
    //调用partitionBy时RDD的Key是(String, String)
    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(sbPatitioner)

    //如果一次拿出一个分区(可以操作一个分区中的数据了)
    val sorted: RDD[((String, String), Int)] = partitioned.mapPartitions(it => {
      //将迭代器转换成list，然后排序，在转换成迭代器返回
      it.toList.sortBy(_._2).reverse.take(topN).iterator
    })

    //
    val r: Array[((String, String), Int)] = sorted.collect()

    println(r.toBuffer)


    sc.stop()


  }
}

//自定义分区器
class SubjectParitioner2(sbs: Array[String]) extends Partitioner {

  //相当于主构造器（new的时候回执行一次）
  //用于存放规则的一个map
  val rules = new mutable.HashMap[String, Int]()
  var i = 0
  for(sb <- sbs) {
    //rules(sb) = i
    rules.put(sb, i)
    i += 1
  }

  //返回分区的数量（下一个RDD有多少分区）
  override def numPartitions: Int = sbs.length

  //根据传入的key计算分区标号
  //key是一个元组（String， String）
  override def getPartition(key: Any): Int = {
    //获取学科名称
    val subject = key.asInstanceOf[(String, String)]._1
    //根据规则计算分区编号,相当于执行apply方法
    rules(subject)
  }
}
