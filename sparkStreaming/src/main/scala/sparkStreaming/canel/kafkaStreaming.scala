package sparkStreaming.canel

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.regexp_extract
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.util.{Date, Properties}

/**
 * @author sluping
 * @create 22/6/5 0:51
 * @Description :
 */
object canalStreaming {

  def main(args: Array[String]): Unit = {
    // offset保存路径
    val checkpointPath = "checkpoints/kafka-direct"

    val conf = new SparkConf()
      .setAppName("ScalaKafkaStreaming")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint(checkpointPath)
    val spark: SparkSession = new SparkSession.Builder().master("local").appName("sqlDemo").getOrCreate()
    val bootstrapServers = "139.198.168.168:9092,139.198.124.160:9092,139.198.116.175:9092"
    val groupId = "flume"
    val topicName = "tmalllog"
    val maxPoll = 500

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )
    case class schema(mytime: String, action: String, frequency: Int)
    val kafkaTopicDS = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topicName), kafkaParams))
    import spark.implicits._
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "bailishuixiang.net")
    properties.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    val uri = "jdbc:mysql://139.198.124.160:3306/tmalldata?useSSL=false"

    kafkaTopicDS.foreachRDD(
      foreachFunc = rdd => if (!rdd.isEmpty()) {
        //数据业务逻辑处理
        val now: Long = new Date().getTime
        val now2: String = now.toString
        val action_df = rdd.map(_.value)
          .map(_.split("-"))
          .filter(x => x.length == 3)
          .map(x => x(2))
          .map(x => (x, 1))
          .reduceByKey(_ + _)
          .map(x => (now2, x._1, x._2))
          .toDF("mytime", "action", "frequency")
        val top_category = action_df.select("*").where("action  like '%分类ID为%'") //.orderBy(action_df("frequency").desc)
        if (top_category.count() > 0) {
          top_category.show()
          top_category.write.mode("append").jdbc(uri, "category", properties)
        }

        val product_Popular_Buy = action_df.select("*").where("action  like '%通过产品ID获取产品信息%'") //.orderBy(action_df("frequency").desc)
        if (product_Popular_Buy.count() > 0) {
          product_Popular_Buy.show()
          product_Popular_Buy.write.mode("append").jdbc(uri, "product", properties)
        }

        val Active_users = action_df.select("*").where("action  like '%用户已登录，用户ID%'") //.orderBy(action_df("frequency"))
        if (Active_users.count() > 0) {
          Active_users.show()
          Active_users.write.mode("append").jdbc(uri, "activeusers", properties)
        }

        val money = action_df.select("*").where("action  like '%总共支付金额为%'") //.orderBy(action_df("frequency").desc)
        val money2 = money.withColumn("single_transaction", regexp_extract($"action", "([0-9]+)", 0))
        if (money2.count() > 0) {
          money2.show()
          money2.write.mode("append").jdbc(uri, "trading", properties)
        }
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }

}
