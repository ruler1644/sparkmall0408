package com.atguigu.handle

import com.alibaba.fastjson.JSONObject
import com.atguigu.datamode.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable

object CategoryTop10Handler {

  def getCategoryTop10Session(userVisitActionRDD: RDD[UserVisitAction], categoryTop10: List[(String, mutable.HashMap[String, Long])]): RDD[(String, String, Long)] = {

    //获取排名前10的品类
    //(1,(click_1,sum1),(order_1,sum1),(pay_1,sum1))
    val categoryList: List[String] = categoryTop10.map(_._1)

    //过滤数据(点击的品类是前10的品类)
    // userVisitAction  click_category_id:1   click_category_id:12
    val filterUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(userVisitAction =>
      categoryList.contains(userVisitAction.click_category_id.toString))

    //转换数据结构,userVisitAction=>((category,sessionId),1L)
    val categoryAndSessionToOne: RDD[((String, String), Long)] = filterUserVisitActionRDD.map(
      userVisitAction =>
        ((userVisitAction.click_category_id.toString, userVisitAction.session_id), 1L))

    //求和,((category,sessionId),1L)=>((category,sessionId),sum)
    val categoryAndSessionToSum: RDD[((String, String), Long)] = categoryAndSessionToOne.reduceByKey(_ + _)

    //转换数据结构,((category,sessionId),sum)=>(category,(sessionId,sum))
    val categoryToSessionAndSum: RDD[(String, (String, Long))] = categoryAndSessionToSum.map {
      case ((category, sessionId), sum) => {
        (category, (sessionId, sum))
      }
    }

    //分组
    //(1,[(s1,20)],(s2,50),(s3,30)...)
    //(2,[(s1,20)],(s2,50),(s3,30)...)
    val categoryToSessionAndSumItr: RDD[(String, Iterable[(String, Long)])] = categoryToSessionAndSum.groupByKey()

    //排序,取出前10
    val sortedCategoryToSessionAndSumItr: RDD[(String, List[(String, Long)])] = categoryToSessionAndSumItr.mapValues {
      case itr =>
        itr.toList.sortWith(_._2 > _._2).take(10)
    }

    //扁平化(1,(s1,20),(s2,14)...)====>(1,s1,20)
    val result: RDD[(String, String, Long)] = sortedCategoryToSessionAndSumItr.flatMap {
      case (category, itr) => {
        val tuples: List[(String, String, Long)] = itr.map {
          case (sessionId, sum) =>
            (category, sessionId, sum)
        }
        tuples
      }
    }
    result
  }


  /**
    * 读取Hive中的数据
    *
    * @param spark   SparkSession
    * @param jsonObj 过滤条件
    *
    */
  def readHiveData(spark: SparkSession, jsonObj: JSONObject): RDD[UserVisitAction] = {

    //隐式转换
    import spark.implicits._

    //获取过滤条件
    val startDate: String = jsonObj.getString("startDate")
    val endDate: String = jsonObj.getString("endDate")
    val startAge: String = jsonObj.getString("startAge")
    val endAge: String = jsonObj.getString("endAge")

    //封装SQL语句
    val sql = new StringBuilder("select ac.* from user_visit_action ac join user_info u on ac.user_id = u.user_id where 1 = 1")

    //拼接过滤条件：时间、字符串类型需要加引号
    if (startDate != null) {
      sql.append(s" and date >= '$startDate'")
    }
    if (endDate != null) {
      sql.append(s" and date <= '$endDate'")
    }
    if (startAge != null) {
      sql.append(s" and age >= $startAge")
    }
    if (endAge != null) {
      sql.append(s" and age <= $endAge")
    }

    //打印SQl------------
    //select ac.* from user_visit_action ac join user_info u on ac.user_id = u.user_id
    //where 1 = 1 and date >= '2019-11-01' and date <= '2019-12-28' and age >= 20 and age <= 50
    // -----------------
    println(sql.toString())

    //读取数据
    val df: DataFrame = spark.sql(sql.toString())

    //ds转为RDD
    val rdd: RDD[UserVisitAction] = df.as[UserVisitAction].rdd
    rdd
  }
}
