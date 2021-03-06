package handler

import java.sql.Timestamp
import java.util.Properties

import com.ggstar.util.ip.IpHelper
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import utils.IpUtil

/**
 * 统计数据
 */
object LogWordCountHandler extends App with Logging{

    val conf = new SparkConf().setMaster("local[*]").setAppName("readLog")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val logRDD: RDD[String] = spark.sparkContext.textFile("my-spark/datas/format.log")
//    logRDD.take(2).foreach(println)
    val requestInfos = logRDD.map(l => {
        val logArray = l.split("\t")
        val ip = logArray(3)
        val city = IpUtil.getCity(ip)
        RequestInfo(logArray(2).toInt, logArray(3), logArray(1), Timestamp.valueOf(logArray(0)), city)
    })
    // ###########################
    // 按城市统计，点击量
//    requestInfos.map(m => (m.city, 1)).reduceByKey(_+_).take(10).foreach(println)
     // ###########################
    // 按请求统计
//    requestInfos.map(m => (m.url, 1)).reduceByKey(_+_).take(10).foreach(println)
    // ###########################
    // 城市和url统计
//    requestInfos.map(m => ((m.city, m.url), 1)).reduceByKey(_+_)
//      .sortBy(_._2, ascending = false)
//      .map(m => ("城市：" + m._1._1, "url：" + m._1._2, "点击量：" + m._2))
//      .take(10).foreach(println)
    // ###########################
    // 先按城市点击量降序排序，城市内按url数量降序排序取城市前三
    requestInfos.map(m => m.city -> List(m.url)).reduceByKey(_:::_)
      .sortBy(_._2.size, ascending = false)
      .mapValues(v => {
          val vn =  v.map((_, 1)).groupBy(_._1).map(t => (t._1, t._2.size))
          vn.toList.sortBy(-_._2).take(3)
      }).flatMapValues(x => x)
      .map{
        case (city, (url, count)) => ("城市：" + city, "url：" + url, "点击量：" +count)}
      .take(100)
      .foreach(println)


    spark.stop()

    case class RequestInfo(traffic: Int, ip: String, url: String, request_time: Timestamp, city: String)



}
