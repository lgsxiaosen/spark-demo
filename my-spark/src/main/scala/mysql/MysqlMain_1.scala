package mysql

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object MysqlMain_1 extends App with Logging{

  val conf = new SparkConf().setMaster("local[*]").setAppName("mysqlApp")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  val jdbcUrl = "jdbc:mysql://localhost:3306/spark?useSSL=false&useUnicode=true&characterEncoding=UTF-8&user=root&password=root"
  val df = spark.read.format("jdbc").options(Map("url" -> jdbcUrl, "dbtable" -> "user")).load()
  println(df.count())
  println(df.rdd.partitions.size)
  import spark.sql
  df.createOrReplaceTempView("user")
  sql("select * from user").show()

  spark.stop()

  case class User(id: Int, username: String, age: Int, password: String, created_at: String)

}
