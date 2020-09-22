package com.atguigu.test

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 累加器尽量不要放在转换算子中，因为转换算子可能被多次复用
  * numToSum.map(),使用累加器后结果是10
  * numToSum.collect(),使用累加器后结果是20
  * numToSum调用两次Action算子，计算了两次，结果不同
  */

object Test {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dfg")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)

    //注册累加器
    val sum: LongAccumulator = sc.longAccumulator("sum")
    val numToSum: RDD[(Int, Int)] = rdd.map(x => {
      sum.add(x)
      (x, 1)
    })

    //打印结果
    numToSum.foreach(println)
    println("**********")

    //第二个行动算子
    numToSum.collect()

    //打印变量
    println(sum.value)
    sc.stop()
  }
}
