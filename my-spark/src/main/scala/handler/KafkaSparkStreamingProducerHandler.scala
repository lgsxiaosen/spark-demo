package handler

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

object KafkaSparkStreamingProducerHandler {

    def main(args: Array[String]): Unit = {
        val prop = new Properties()
        // 指定请求的kafka集群列表
        prop.put("bootstrap.servers", "localhost:9092")// 指定响应方式
        //prop.put("acks", "0")
        prop.put("acks", "all")
        // 请求失败重试次数
        //prop.put("retries", "3")
        // 指定key的序列化方式, key是用于存放数据对应的offset
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        // 指定value的序列化方式
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        // 配置超时时间
        prop.put("request.timeout.ms", "60000")
        //prop.put("batch.size", "16384")
        //prop.put("linger.ms", "1")
        //prop.put("buffer.memory", "33554432")

        val topic = "kafkaTopic"
        // 得到生产者的实例
        val producer = new KafkaProducer[String, String](prop)

        // 模拟一些数据并发送给kafka
        for (i <- 1 to 100) {
            val msg = s"${i}: this is a line ${i} kafka data"
            println("send -->" + msg)
            // 得到返回值
            val rmd: RecordMetadata = producer.send(new ProducerRecord[String, String](topic, msg)).get()
            println(rmd.toString)
            Thread.sleep(500)
        }
        /*producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfiguration)
        while (iterator.hasNext && failedWrite == null) {
          val currentRow = iterator.next()
          // 这里的 projection 主要是构建 projectedRow，使得：
          // 其第 0 号元素是 topic
          // 其第 1 号元素是 key 的 binary 表示
          // 其第 2 号元素是 value 的 binary 表示
          val projectedRow = projection(currentRow)
          val topic = projectedRow.getUTF8String(0)
          val key = projectedRow.getBinary(1)
          val value = projectedRow.getBinary(2)
          if (topic == null) {
            throw new NullPointerException(s"null topic present in the data. Use the " +
            s"${KafkaSourceProvider.TOPIC_OPTION_KEY} option for setting a default topic.")
          }
          val record = new ProducerRecord[Array[Byte], Array[Byte]](topic.toString, key, value)
          // 回调信息
          val callback = new Callback() {
            override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
              if (failedWrite == null && e != null) {
                failedWrite = e
              }
              println(recordMetadata)
            }
          }
          producer.send(record, callback)
    }*/
        producer.close()
    }

}
