package day03



/**
  *
  * 根據學科取得最受欢迎的老师前2名
  */
import java.net.URL
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavTeacherWithObject {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val conf = new SparkConf()
    conf.setAppName("FavTeacher").setMaster("local[2]") //local[*]表示用多个线程跑，2表示用两个线程
    val sc = new SparkContext(conf)

    //读取数据
    val lines: RDD[String] = sc.textFile("D:\\data\\teacher.log")

    //整理数据，每个老师记一次数
    val subjectAddTeacher: RDD[((String, String), Int)] = lines.map(line => {
      val teacher = line.substring(line.lastIndexOf("/") + 1)
      val url = new URL(line).getHost
      val subject = url.substring(0, url.indexOf("."))
      ((subject, teacher), 1)
    })

    //聚合
    val reduced: RDD[((String, String), Int)] = subjectAddTeacher.reduceByKey(_+_)
    println(reduced.collect().toBuffer)

    //根據學科進行 分組
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)

    println(grouped.collect().toBuffer)

    //排序，这里的排序取前两名， 取到的数据是scala集合list中进行排序的
    //先分组，在组内进行排序，这CompactBuffer是迭代器，继承了序列，然后将迭代器转换成list进行排序
    //在某种极端的情况，_表示迭代分区的数据，这里是将迭代器的数据一次性的拉去过来后进行toList,如果数据量非常的大，这里肯定会出现OOM（内存溢出）
    val sorted: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(_.toList.sortBy( - _._2).take(2))

    //println(sorted.collect().toBuffer)

    val result = sorted.collect()
    result.foreach(println)

    //释放资源
    sc.stop()
  }
}
