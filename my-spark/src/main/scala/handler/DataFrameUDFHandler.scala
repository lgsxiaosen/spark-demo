package handler

import java.sql.{Date, Timestamp}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

object DataFrameUDFHandler {

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
        import org.apache.spark.sql.functions._
        val longLength = udf((id: Int, length: Int) => id > length)
        saleDF.filter(longLength($"id", lit(5))).show()
        spark.udf.register("my_avg", udaf(new AvgUdf))
        saleDF.createOrReplaceTempView("sales_tmp")
        val frame = spark.sql("select province, count(province) as count, sum(sales) as sum_sales, my_avg(sales) as avg_sales from sales_tmp group by province")
        frame.show()

    }

case class Buffer(sales: Double, count: Int)

class AvgUdf extends Aggregator[Double, Buffer, Double]{

    // 初始值
    override def zero: Buffer = {
        Buffer(0d, 0)
    }

    // 每个分组区局部聚合的方法
    override def reduce(b: Buffer, a: Double): Buffer = {
        Buffer(b.sales + a, b.count + 1)
    }

    // 全局聚合调用的方法
    override def merge(b1: Buffer, b2: Buffer): Buffer = {
        Buffer(b1.sales + b2.sales, b1.count + b2.count)
    }

    // 计算最终的结果
    override def finish(reduction: Buffer): Double = {
        reduction.sales / reduction.count
    }

    // 中间结果的encoder
    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    // 返回结果的encoder
    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

}
