package handler

import handler.LogWordCountHandler.conf
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaSparkStreamingConsumerHandler {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("KafkaSparkStreamingConsumerHandler").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(sparkConf)
        sc.setLogLevel("WARN")
        val streamingContext: StreamingContext = new StreamingContext(sc, Seconds(5))

        //创建topic
        val brokers = "localhost:9092"
        val topic = "kafkaTopic";
        //创建消费者组
        val group = "sparkafkaGroup"
        //消费者配置
        val kafkaParam = Map(
            "bootstrap.servers" -> brokers, //用于初始化链接到集群的地址
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            //用于标识这个消费者属于哪个消费团体
            "group.id" -> group,
            //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
            //可以使用这个配置，latest自动重置偏移量为最新的偏移量
            //auto.offset.reset设置为smallest，不然启动的时候为largest，只能收取实时消息
            "auto.offset.reset" -> "latest",
            //如果是true，则这个消费者的偏移量会在后台自动提交
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )
        //接收kafka里面的数据
        val directStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String,String](Array(topic), kafkaParam))

        //使用foreachrdd循环遍历每一个rdd当中的数据
        directStream.foreachRDD(x=>{
            //x.count()>0就证明我们的rdd中有数据
            if (x.count()>0){
                //获取数据值
                x.foreach(f=>{
                    val value: String = f.value()
                    println(value)
                })
                //消费了kafka中的数据后提交offset信息,获取消费完之后的offset的值
                //将rdd中的offset数据都取出来进行提交
                val offsetRanges: Array[OffsetRange] = x.asInstanceOf[HasOffsetRanges].offsetRanges
                //异步提交offset
                directStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
            }
        })

        /*directStream.map(_.value()).flatMap(_.split(" ")).foreachRDD(rdd => {
            val sqlContext = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
            import sqlContext.implicits._
            val wordsDF = rdd.toDF("word")
            wordsDF.createOrReplaceTempView("words")
            val wordsCount = sqlContext.sql("select word,count(*) from words group by word")
            wordsCount.show()
        })*/





        streamingContext.start()
        streamingContext.awaitTermination()

    }

}
