package day06

import java.sql.{Connection, DriverManager}

import org.apache.log4j.{Level, Logger}
import org.apache.log4j.Logger
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JDBCDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    //连接数据库
    val conn = () => {
      DriverManager.getConnection( "jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "926718")

    }
    val conf = new SparkConf().setAppName("JDBCDemo").setMaster("local")
    val sc = new SparkContext(conf)
    /**
      * JdbcRDD有6个参数 。
        1、 sc SparkContext 类型变量
        2、链接 jdbc的链接对象
        3、sql语句，一般为查询语句
        4和5 、为上下边界。
        6、partitions 分区数
        最后一个 参数，里面存放的是执行sql语句的返回值。
      */
    val jdbcRDD = new JdbcRDD(
      sc,
      conn,
      "select id, name, age from logs where id>=? and id<=?",
      1,
      5,
      2,
      rs => {
        val id = rs.getDouble(1)
        val name = rs.getString(2)
        val age = rs.getInt(3)
        (id,name,age)
      }

    )
    val result = jdbcRDD.collect()
    println(result.toBuffer)
    sc.stop()

  }
}
