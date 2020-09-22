package handler

import com.atguigu.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object Top3DateAreaAdCountHandler {

  //每天各地区热门广告Top3，并将其写入Redis
  def saveDateAreaTop3AdCountToRedis(dateAreaCityToCount: DStream[((String, String, String, String), Long)]) = {

    //去掉城市维度
    val dateAreaAdToCount1: DStream[((String, String, String), Long)] = dateAreaCityToCount.map {
      case ((date, area, city, ad), count) => {
        ((date, area, ad), count)
      }
    }

    //聚合数据
    val dateAreaAdToCount: DStream[((String, String, String), Long)] = dateAreaAdToCount1.reduceByKey(_ + _)

    //转换数据结构
    val dateAreaToAdCount: DStream[((String, String), (String, Long))] = dateAreaAdToCount.map {
      case ((date, area, ad), count) => {
        ((date, area), (ad, count))
      }
    }

    //分组
    val dateAreaToAdCountList: DStream[((String, String), Iterable[(String, Long)])] = dateAreaToAdCount.groupByKey()

    //排序，取前3
    val dateAreaToAdCountTop3: DStream[((String, String), List[(String, Long)])] = dateAreaToAdCountList.mapValues(
      items => {
        items.toList.sortWith(_._2 > _._2).take(3)
      })

    //TODO 将各地区热门广告前3，换成json格式
    val dateAreaToJson: DStream[((String, String), String)] = dateAreaToAdCountTop3.mapValues(items => {

      import org.json4s.JsonDSL._
      JsonMethods.compact(items)
    })

    //将数据写入Redis，一次写入一条数据
    dateAreaToJson.foreachRDD(rdd => {
      rdd.foreachPartition(items => {

        //获取Redis连接
        val jedis: Jedis = RedisUtil.getJedisClient

        items.foreach { case ((date, area), jsonStr) =>
          val redisKey = s"top3_ads_per_day:$date"

          //写入数据格式如下
          // key:  top3_ads_per_day:2019-11-26
          // field:   华北
          // value:  {“2”:1200, “9”:1100, “13”:910}
          jedis.hset(redisKey, area, jsonStr)
        }

        //关闭Redis连接
        jedis.close()
      })
    })
  }

}
