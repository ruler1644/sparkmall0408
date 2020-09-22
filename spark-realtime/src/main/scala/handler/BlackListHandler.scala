package handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import bean.AdsLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListHandler {

  //时间转换
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  //黑名单的redisKey
  private val blackList = "BlackList"

  //根据黑名单过滤数据集
  def filterDataByBlackList(sc: SparkContext, adsLogDStream: DStream[AdsLog]): DStream[AdsLog] = {
    adsLogDStream.transform(rdd => {

      /*
            //操作Redis太频繁了
            //获取连接是针对每个分区进行一次，但是filter过滤操作针对每条数据进行一次
            //而且黑名单的动态改变是在checkDataToBackList方法中，在filterDataByBlackList方法中
            //数据位于同一个批次，黑名单的内容不会发生变动，所以可以获取黑名单后再比较，不需要太频繁
            rdd.foreachPartition(items => {

              //获取Redis连接
              val jedis = RedisUtil.getJedisClient
              items.filter(x => jedis.sismember(blackList, x.userid))

              //关闭Redis连接
              jedis.close()
            })
      */

      val jedisClient: Jedis = RedisUtil.getJedisClient

      //transform算子内部获取黑名单(Driver端，每个批次执行一次)
      val blackListUserids: util.Set[String] = jedisClient.smembers(blackList)
      jedisClient.close()

      //将黑名单做成广播变量，一个批次只需要和Redis交互一次
      val blackListBC: Broadcast[util.Set[String]] = sc.broadcast(blackListUserids)

      //校验用户是否在黑名单中
      rdd.filter(AdsLog => !blackListBC.value.contains(AdsLog.userid))
    })
  }

  //过滤后的数据集再次校验
  def checkDataToBackList(adsLogDStream: DStream[AdsLog]): Unit = {

    //每天每个人点击每个广告的次数
    val dataUserAdCount: DStream[((String, String, String), Long)] = adsLogDStream.map(
      adsLog => {

        val date: String = sdf.format(new Date(adsLog.timestamp))
        ((date, adsLog.userid, adsLog.adid), 1L)
      }).reduceByKey(_ + _)


    //将点击次数，集合Redis中现存的数据统计总数
    //不需要返回值，对rdd进行操作
    dataUserAdCount.foreachRDD(rdd => {

      rdd.foreachPartition(items => {

        //获取Redis客户端
        val jedis: Jedis = RedisUtil.getJedisClient

        items.foreach { case ((date, userid, adid), count) =>

          //RedisKey每天生成一个，数据过期后不想保存，可以手动删除。
          //获取Redis已经保存的数据量，和新产生的点击次数累加与100比较，如：46 + 8
          val redisKey = s"date_user_ad_$date"
          val hashKey = s"$userid:$adid"

          //将数据进行汇总
          jedis.hincrBy(redisKey, hashKey, count)

          //校验点击次数是否超过100
          if (jedis.hget(redisKey, hashKey).toLong >= 100L) {

            //将该用户加入黑名单
            jedis.sadd(blackList, userid)
          }
        }

        //关闭Redis连接
        jedis.close()
      })
    })
  }

}
