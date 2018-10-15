package day14

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * 一个简单的Rdis连接池
  */
object JPools {
    private val poolConfig = new GenericObjectPoolConfig()
    poolConfig.setMaxIdle(5)    //最大的空间连接数，连接池中最大的空闲连接数，默认是8
    poolConfig.setMaxTotal(2000) //只支持最大的连接数，连接池中最大的连接数，默认是8


  //连接池是私有的不能对外公开访问
  private lazy val jedisPool = new JedisPool(poolConfig,"hadoop02")


  /**
    * 对外提供一个可以从连接池获取连接的方法
    */

  def getJedis = {
    val jedis = jedisPool.getResource
    jedis.select(0)
    jedis
  }
}
