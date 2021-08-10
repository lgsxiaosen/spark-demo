package handler

import java.sql.{Date, Timestamp}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataFrameHandler {

    implicit class StringFuncs(str: String) {
        def toTimestamp = new Timestamp(Date.valueOf(str).getTime)
    }

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("dataframe")
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        import spark.implicits._
        val sales = Seq(
            (1, "Widget Co", 1000.00, 0.00, "广东省", "深圳市", "2014-02-01".toTimestamp),
            (2, "Acme Widgets", 1000.00, 500.00, "四川省", "成都市", "2014-02-11".toTimestamp),
            (3, "Acme Widgets", 1000.00, 500.00, "四川省", "绵阳市", "2014-02-12".toTimestamp),
            (4, "Acme Widgets", 1000.00, 500.00, "四川省", "成都市", "2014-02-13".toTimestamp),
            (5, "Widget Co", 1000.00, 0.00, "广东省", "广州市", "2015-01-01".toTimestamp),
            (6, "Acme Widgets", 1000.00, 500.00, "四川省", "泸州市", "2015-01-11".toTimestamp),
            (7, "Widgetry", 1000.00, 200.00, "四川省", "成都市", "2015-02-11".toTimestamp),
            (8, "Widgets R Us", 3000.00, 0.0, "四川省", "绵阳市", "2015-02-19".toTimestamp),
            (9, "Widgets R Us", 2000.00, 0.0, "广东省", "深圳市", "2015-02-20".toTimestamp),
            (10, "Ye Olde Widgete", 3000.00, 0.0, "广东省", "深圳市", "2015-02-28".toTimestamp),
            (11, "Ye Olde Widgete", 3000.00, 0.0, "广东省", "广州市", "2015-02-28".toTimestamp))
        val saleDF = spark.sparkContext.parallelize(sales, 4).toDF("id", "name", "sales", "discount", "province", "city", "saleDate")
        saleDF.createOrReplaceTempView("sales_tmp")

        val dataFrame = spark.sql("select province,city,sales from sales_tmp")
        dataFrame.show

        val resultDF = dataFrame.rollup($"province", $"city").agg(Map("sales" -> "sum"))
        resultDF.show


    }


}
