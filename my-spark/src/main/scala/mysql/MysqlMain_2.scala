package mysql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object MysqlMain_2 extends App with Logging{

  val conf = new SparkConf().setMaster("local[*]").setAppName("mysqlApp")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  val prop = new Properties()
  val jdbcUrl = "jdbc:mysql://localhost:3306/spark?useSSL=false&useUnicode=true&characterEncoding=UTF-8"
  prop.put("user", "root")
  prop.put("password", "root")
  // 分区
  val lowerBound = 1
  val upperBound = 2
  val numPartitions = 5
  val df: DataFrame = spark.read.jdbc(jdbcUrl, "user", "id", lowerBound, upperBound, numPartitions, prop)
  println(df.count())
  println(df.rdd.partitions.size)

  spark.stop()

  case class User(id: Int, username: String, age: Int, password: String, created_at: String)

}
