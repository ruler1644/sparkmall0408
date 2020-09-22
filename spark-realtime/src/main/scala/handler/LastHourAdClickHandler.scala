package handler

import java.text.SimpleDateFormat
import java.util.Date

import bean.AdsLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object LastHourAdClickHandler {

  private val sdf = new SimpleDateFormat("HH:mm")
  private val redisKey = "last_hour_ads_click"

  //最近一小时每分钟广告点击量
  def saveLastHourAdCountToRedis(filterAdsLogDStream: DStream[AdsLog]) = {

    //过滤字段
    val adHourMinToOne: DStream[((String, String), Long)] = filterAdsLogDStream.map(AdsLog => {

      //获取小时和分钟
      val hourMin: String = sdf.format(new Date(AdsLog.timestamp))
      ((AdsLog.adid, hourMin), 1L)
    })

    //开窗聚合数据，为迅速看到效果，窗口大小设定为3分钟
    val adHourMinToCount: DStream[((String, String), Long)] = adHourMinToOne.reduceByKeyAndWindow((x: Long, y: Long) => x + y, Minutes(3))

    //转换数据结构
    val adToHourMinCount: DStream[(String, (String, Long))] = adHourMinToCount.map {
      case ((ad, hourMin), count) => {
        (ad, (hourMin, count))
      }
    }

    //分组
    val adToHourMinCountList: DStream[(String, Iterable[(String, Long)])] = adToHourMinCount.groupByKey()

    //将数据转换成jsonStr
    val adToHourMinCountStr: DStream[(String, String)] = adToHourMinCountList.mapValues(items => {

      import org.json4s.JsonDSL._
      JsonMethods.compact(items.toList)
    })

    //将数据写入Redis
    adToHourMinCountStr.foreachRDD(rdd => {

      //统一清理
      val jedis: Jedis = RedisUtil.getJedisClient
      if (jedis.exists(redisKey)) {
        jedis.del(redisKey)
      }
      jedis.close()


      rdd.foreachPartition(items => {

        //获取Redis连接
        val jedis: Jedis = RedisUtil.getJedisClient

        //        //写入数据
        //        items.foreach { case (ad, jsonStr) =>
        //          jedis.hset(redisKey, ad, jsonStr)
        //        }

        //批量保存
        if (items.nonEmpty) {
          import scala.collection.JavaConversions._
          jedis.hmset(redisKey, items.toMap)
        }

        //关闭Redis连接
        jedis.close()
      })
    })
  }
}
