package caseTest
/**
  *
  *
  *                           ipAddress: String, // IP地址
                              clientId: String, // 客户端唯一标识符
                              userId: String, // 用户唯一标识符
                              serverTime: String, // 服务器时间
                              method: String, // 请求类型/方式
                              endpoint: String, // 请求的资源
                              protocol: String, // 请求的协议名称
                              responseCode: Int, // 请求返回值：比如：200、401
                              contentSize: Long // 返回的结果数据大小
  *
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object NeedAllWithTeacher {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf()
    conf.setAppName("Need1").setMaster("local")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("D:\\data\\in\\access_2013_05_31.log")
    //对数据进行过滤，空的过滤出去
    val apacheAccessLog = lines.filter(line => ApacheAccessLog.isValidateLogLine(line)).map(line => {
      //对数据进行转换处理
      val log = ApacheAccessLog.parseLogLine(line)
      log
    })
    //对多次使用的rdd进行cache
    apacheAccessLog.cache()
    //需求一：求contentSize(返回的结果数据大小)的平均值、最小值、最大值
    //提取计算所需要的字段
    val contentSizeRDD = apacheAccessLog.map(log => {
      log.contentSize
    })
    //对重复使用的RDD进行cache
    contentSizeRDD.cache()

    //开始计算平均值，最小值、最大值
    val totalContentSize = contentSizeRDD.sum()
    val totalCount = contentSizeRDD.count()
    val avgSize = totalContentSize * 1.0 / totalCount
    val minSize = contentSizeRDD.min()
    val maxSize = contentSizeRDD.max()

    //当RDD不使用的时候，进行unpersist释放缓存
    contentSizeRDD.unpersist()
    println(s"ContentSize Avg:${avgSize},Min: ${minSize},Max: ${maxSize}")

    //需求二：求各个不同返回值的出现的数据

    //提取需要的字段数据，转换为key/value ，方便进行reduceBykey操作
    //使用reduceBykey函数，按照key进行分组后，计算每个Key出现的次数
    val responseCodeResultRDD = apacheAccessLog.map(log => (log.responseCode, 1)).reduceByKey(_ + _)
    //结果输出
    println(s"ResponseCode: ${responseCodeResultRDD.collect().mkString(",")}")


    //需求三：获取访问次数超过N次的IP地址
    //需求三额外：对IP地址进行限制，部分黑名单IP地址不统计
    val blackIP = Array("200-55-104-193.dsl.prima.net.ar","208-38-57-205.ip.cal.raddiant.net")
    //由于集合比较大，将集合的内容广播出去
    val broadCastIP = sc.broadcast(blackIP)
    val N = 10
    val ipAddressRDD = apacheAccessLog
      //过滤地址在黑名单中的数据
      .filter(log => !broadCastIP.value.contains(log.ipAddress))
      //获取计算需要的IP地址数据，并将返回值转换为key/value键值对类型
      .map(log => (log.ipAddress,1L))
    //使用reduceBykey函数进行聚合操作
      .reduceByKey(_+_)
    //过滤数据，要求IP地址必须出现N次以上
      .filter(tuple => tuple._2>N)
    //获取满足条件的IP地址，为了展示方便，将下面这行代码注释
    //    .map(tuple=> tuple._1)
    //结果输出
    println(s"""IP Address :${ipAddressRDD.collect().mkString(",")}""")



    //需求四： 获取访问次数最多的前K个endpoint的值 =====> TopN
    val K = 10
    val topKValues = apacheAccessLog
    //获取计算需要的IP地址数据，并将返回值转换为Key/value键值对类型
      .map(log => (log.endpoint,1))
    //获得每个enpoint对应的出现次数
      .reduceByKey(_+_)
    //获取前10个元素，而且使用我们自定义的排序类
      .top(K)(LogSorting.TupleOrdering)
    //如果只需要endpoint的值，不需要出现次数，那么可以通过map函数进行转换
     //   .map(_._1)
    //结果输出
    println(s"""TopK values:${topKValues.mkString(",")}""")
    //对不再使用的RDD，去除cache

    sc.stop()


  }
}
