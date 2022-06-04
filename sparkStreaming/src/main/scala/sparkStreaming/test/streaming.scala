package sparkStreaming.test

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object streamingtest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkFlumeNG")
    val ssc = new StreamingContext(conf, Seconds(30))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "master:9092,slave1:9092,slave2:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "fwmagic",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topic = Set("test")

    val data = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topic,kafkaParams)
    )
    data.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        //数据业务逻辑处理
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
