package sparkStreaming.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Date

object process {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("test")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark: SparkSession = new SparkSession.Builder().master("local").appName("sqlDemo").getOrCreate()
    sc.setLogLevel("WARN")
    var now: Long = new Date().getTime
    import spark.implicits._
    val rdd = sc.textFile("G:/mytaks/hadoop/spark/sparkstreamingdemo/src/main/resources/info.log")
    //过滤得到长度仅为3的记录 取记录最后一个值
    val rdd1 = rdd.map(x => x.split("-")).filter(x => x.length == 3).map(x => x(2))
    //用户行为表
    val action_df = rdd1.map(x => (x, 1)).reduceByKey(_ + _).map(x => (now, x._1, x._2)).toDF("mytime", "action", "frequency") //.show()
    val top_category = action_df.select("*").where("action  like 'cao'").orderBy(action_df("frequency").desc)
    if (top_category.select("*").count()>0){top_category.show()}
    else {print("no")}
//    top_category.show()
//    val product_Popular_Buy = action_df.select("*").where("action  like '%通过产品ID获取产品信息%'").orderBy(action_df("frequency").desc)
//    product_Popular_Buy.show()
//    val Active_users = action_df.select("*").where("action  like '%用户已登录，用户ID%'").orderBy(action_df("frequency"))
//    Active_users.show()
//    val money = action_df.select("*").where("action  like '%总共支付金额为%'").orderBy(action_df("frequency").desc)
//    val money2 = money.withColumn("single_transaction", regexp_extract($"action", "\\d+", 0))
//    money2.show()
    //val totalmoney: Unit = money2.select(sum(money2("money").cast("int") * money2("frequency"))).show()
    // .show()//.foreach(print)
    //用户操作时间表
    //val time_df = rdd1.map(x => x(0)).map(x => (x, 1)).reduceByKey(_ + _).sortByKey(ascending = false).toDF("time", "frequency")
    //time_df.show()
    //用户访问网址
    //val path_df = rdd1.map(x => x(1)).map(x => (x, 1)).reduceByKey(_ + _).sortByKey(ascending = false).toDF("path", "frequency")
    //path_df.show()//.foreach(print)

  }

}
