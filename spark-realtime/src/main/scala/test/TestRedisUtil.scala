package test

import com.atguigu.utils.RedisUtil
import redis.clients.jedis.Jedis

object TestRedisUtil {
  def main(args: Array[String]): Unit = {


    val client: Jedis = RedisUtil.getJedisClient
    println(client)
  }
}
