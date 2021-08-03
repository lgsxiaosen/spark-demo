package handler

import java.sql.{Date, Timestamp}
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
 * 读取日志文件，写入mysql数据库
 */
object ReadLogHandler extends App with Logging{

    val conf = new SparkConf().setMaster("local[*]").setAppName("readLog")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val logRDD: RDD[String] = spark.sparkContext.textFile("my-spark/datas/format.log")
//    logRDD.take(2).foreach(println)
    val requestInfos = logRDD.map(l => {
        val logArray = l.split("\t")
        RequestInfo(logArray(2).toInt, logArray(3), logArray(1), Timestamp.valueOf(logArray(0)))
    }).toDS()
    val writeData: Dataset[RequestInfo] = requestInfos.distinct()
    val prop = new Properties()
    val jdbcUrl = "jdbc:mysql://localhost:3306/spark?useSSL=false&useUnicode=true&characterEncoding=UTF-8"
    prop.put("user", "root")
    prop.put("password", "root")
    requestInfos.write.mode(SaveMode.Append).jdbc(jdbcUrl, "request_info", prop)


    spark.stop()

    case class RequestInfo(traffic: Int, ip: String, url: String, request_time: Timestamp)



}
