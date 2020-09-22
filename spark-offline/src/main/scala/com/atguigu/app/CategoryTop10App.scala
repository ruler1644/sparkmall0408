package com.atguigu.app

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.accu.MyAccumulator1
import com.atguigu.datamode.UserVisitAction
import com.atguigu.handle.CategoryTop10Handler
import com.atguigu.utils.{JdbcUtil, PropertiesUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object CategoryTop10App {
  def main(args: Array[String]): Unit = {

    //获取SparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("h")

    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    //TODO 需求一：在符合条件的session中，获取点击、下单和支付数量排名前10的品类
    //获取配置信息， 即session的范围：startDate-->endDate, startAge-->endAge
    val properties: Properties = PropertiesUtil.load("conditions.properties")
    val jsonStr: String = properties.getProperty("condition.params.json")
    val jsonObj: JSONObject = JSON.parseObject(jsonStr)

    //按照要求读取数据，并转换为RDD
    val userVisitActionRDD: RDD[UserVisitAction] = CategoryTop10Handler.readHiveData(spark, jsonObj)

    //创建累加器对象
    // val accu = new CategoryAccu
    val accu = new MyAccumulator1

    //注册累加器
    spark.sparkContext.register(accu, "categoryCount")

    //使用累加器统计各品类：点击，下单，支付的次数
    userVisitActionRDD.foreach(UserVisitAction => {

      //a.判断是否是点击日志====-1====>click_1
      if (UserVisitAction.click_category_id != -1) {
        accu.add(s"click_${UserVisitAction.click_category_id}")
      }

      //b.判断是否是订单日志====9,1,19,7,14====>order_1
      else if (UserVisitAction.order_category_ids != null) {

        //遍历订单日志中的order_category_ids
        UserVisitAction.order_category_ids.split(",").foreach(category => {
          accu.add(s"order_$category")
        })
      }

      //c.判断是否是支付日志====9,1,19,7,14====>pay_1
      else if (UserVisitAction.pay_category_ids != null) {

        //遍历支付日志中的pay_category_ids
        UserVisitAction.pay_category_ids.split(",").foreach(category => {
          accu.add(s"pay_$category")
        })
      }
    })

    //获取累加器中的数据(click_1,sum)(click_2,sum)(order_1,sum)(pay_1,sum)
    val categorySum: mutable.HashMap[String, Long] = accu.value

    //规整数据===>>>(1,(click_1,sum1),(order_1,sum1),(pay_1,sum1))
    val categoryToCategorySum: Map[String, mutable.HashMap[String, Long]] = categorySum.groupBy(_._1.split("_")(1))


    //排序，取出前10，====(1,(click_1,sum1),(order_1,sum1),(pay_1,sum1))
    val categoryTop10: List[(String, mutable.HashMap[String, Long])] = categoryToCategorySum.toList.sortWith {
      case (c1, c2) => {

        //获取参与比较的数据(click_1,sum),(order_1,sum),(pay_1,sum)
        val categorySum1: mutable.HashMap[String, Long] = c1._2
        val categorySum2: mutable.HashMap[String, Long] = c2._2

        //比较的逻辑：
        //先支付，
        if (categorySum1.getOrElse(s"pay_${c1._1}", 0L) > categorySum2.getOrElse(s"pay_${c2._1}", 0L)) {
          true
        } else if (categorySum1.getOrElse(s"pay_${c1._1}", 0L) == categorySum2.getOrElse(s"pay_${c2._1}", 0L)) {

          //再订单，
          if (categorySum1.getOrElse(s"order_${c1._1}", 0L) > categorySum2.getOrElse(s"order_${c2._1}", 0L)) {
            true
          } else if (categorySum1.getOrElse(s"order_${c1._1}", 0L) == categorySum2.getOrElse(s"order_${c2._1}", 0L)) {

            //最后点击
            categorySum1.getOrElse(s"click_${c1._1}", 0L) > categorySum2.getOrElse(s"click_${c2._1}", 0L)
          } else {
            false
          }
        } else {
          false
        }
      }
    }.take(10)
    println("categoryTop10对象：" + categoryTop10)
    //List((1,Map(order_1 -> 633, click_1 -> 2130, pay_1 -> 491)),...)

    //封装Array参数
    val result: List[Array[Any]] = categoryTop10.map {
      case (category_id, categoryMap) => {
        Array(
          s"aaa--${System.currentTimeMillis()}",
          category_id,
          categoryMap.getOrElse(s"click_${category_id}", 0L),
          categoryMap.getOrElse(s"order_${category_id}", 0L),
          categoryMap.getOrElse(s"pay_${category_id}", 0L)
        )
      }
    }
    println("封装Array参数" + result)

    //写入MySQl
    //JdbcUtil.executeUpdate("insert into category_top10 values(?,?,?,?,?)", Array("aaa", "12", 10, 8, 5))
    JdbcUtil.executeBatchUpdate("insert into category_top10 values(?,?,?,?,?)", result)

    //缓存
    userVisitActionRDD.cache()

    //TODO 需求二：对于排名前 10 的品类，分别获取其点击次数排名前 10 的 SessionId

    //数据形式：(category,sessionId,sum)
    val categoryAndSessionIdAndSum: RDD[(String, String, Long)] = CategoryTop10Handler.getCategoryTop10Session(userVisitActionRDD, categoryTop10)

    //转换数据格式，封装成Array
    val categoryAndSessionIdAndSumArr: RDD[Array[Any]] = categoryAndSessionIdAndSum.map {
      case (category, sessionId, sum) => {
        Array(
          s"aaa---${System.currentTimeMillis()}",
          category,
          sessionId,
          sum
        )
      }
    }

    //将数据拉取到Driver端，使之符合Jdbc参数格式(RDD-->Array)
    val sessionTop10Arr: Array[Array[Any]] = categoryAndSessionIdAndSumArr.collect()
    JdbcUtil.executeBatchUpdate("insert into category_session_top10 values(?,?,?,?)", sessionTop10Arr)

    //关闭资源
    spark.stop()
  }
}
