package mysql

import java.sql.{PreparedStatement, ResultSet}
import java.util.Properties

import mysql.utils.MysqlConnection
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object MysqlMain extends App with Logging{

  val conf = new SparkConf().setMaster("local[*]").setAppName("mysqlApp")
  val spark = SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._
//  val conn = MysqlConnection.getConnection
//  val statement: PreparedStatement = conn.prepareStatement("select * from user")
//  val res: ResultSet = statement.executeQuery()
//
//
//  MysqlConnection.releaseConnection(conn)
  val prop = new Properties()
  val jdbcUrl = "jdbc:mysql://localhost:3306/spark?useSSL=false&useUnicode=true&characterEncoding=UTF-8"
  prop.put("user", "root")
  prop.put("password", "root")
  val df: DataFrame = spark.read.jdbc(jdbcUrl, "user", prop)
//  df.show()
//  df.filter($"age">20).show()
//  df.select("username").show()
//  df.selectExpr("max(age)").show()
//  df.select(df("id"), df("age")).show()
//  df.orderBy(df("id")).show()
//  df.orderBy(- df("id")).show()
//  df.agg("age" -> "max").show()
//  df.agg("age" -> "avg").show()
  df.groupBy("age").max().show()


  spark.stop()

}
