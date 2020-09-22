package handler

import java.text.SimpleDateFormat
import java.util.Date

import bean.AdsLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DateAreaCityAdCountHandler {


  //时间转换
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  //获取每天每个大区各个城市广告点击总数
  def getDateAreaCityCount(filterAdsLogDStream: DStream[AdsLog]) = {

    //转换数据格式
    val dateAreaCityAdToOne: DStream[((String, String, String, String), Long)] = filterAdsLogDStream.map(AdsLog => {

      val date = sdf.format(new Date(AdsLog.timestamp))
      ((date, AdsLog.area, AdsLog.city, AdsLog.adid), 1L)
    })

    //使用算子updateStateByKey，有状态的统计总数
    val dateAreaCityAdToCount: DStream[((String, String, String, String), Long)] =
      dateAreaCityAdToOne.updateStateByKey((seq: Seq[Long], status: Option[Long]) => {

        //统计当前批次的总数
        val sum: Long = seq.sum

        //将当前批次与之前状态中的数据合并
        Some(sum + status.getOrElse(0L))
      })

    //返回
    dateAreaCityAdToCount
  }

  //将每天每个大区各个城市广告点击总数写入Redis
  def saveDateAreaCityAdCountToRedis(dateAreaCityToCount: DStream[((String, String, String, String), Long)]) = {


    //写入Redis(单条数据)
    dateAreaCityToCount.foreachRDD(rdd => {
      rdd.foreachPartition(items => {

        //获取Redis连接
        val jedis: Jedis = RedisUtil.getJedisClient

        //("date_area_city_ad_2020-09-19",("华北_北京_2",37))
        items.foreach { case ((date, area, city, adid), count) =>

          //hash结构
          val redisKey = s"date_area_city_ad_$date"
          val hashKey = s"$area:$city:$adid"
          jedis.hset(redisKey, hashKey, count.toString)
        }

        //关闭连接
        jedis.close()
      })
    })

    /*
     //写入Redis(批量写入)
      dateAreaCityToCount.foreachRDD(rdd => {
        rdd.foreachPartition(items => {

          //获取Redis连接
          val jedis: Jedis = RedisUtil.getJedisClient
          val dateToAreaCityAdCountMap: Map[String, Map[(String, String, String, String), Long]] = items.toMap.groupBy(_._1._1)
          val dateToAreaCityAdCountResultMap: Map[String, Map[String, String]] = dateToAreaCityAdCountMap.map { case (date, items) =>
            val stringToLong: Map[String, String] = items.map { case ((date2, area, city, ad), count) =>
              (s"$area:$city:$ad", count.toString)
            }
            (date, stringToLong)
          }

          dateToAreaCityAdCountResultMap.foreach {
            case (date, map) => {
              val redisKey = s"date_area_city_ad_$date"

              //导入java和scala间的隐式转换
              import scala.collection.JavaConversions._
              jedis.hmset(redisKey, map)
            }
          }

          //关闭连接
          jedis.close()
        })
      })
   */


  }
}