package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, SparkConf, SparkContext}


object operators {
  val sc: SparkContext = initSparkContext()
  def main(args: Array[String]): Unit = {
    operator()
  }
  def operator(): Unit ={
    val rdd = readFile()
    //计数
//    println(rdd.count())
    //遍历
    //rdd.foreach(println)
    //遍历处理
//    val rdd_1 = rdd.map(row=>{
//      val arr = row.split(",")
//      arr(0)
//    })
//    rdd_1.foreach(println)
    //遍历处理 将本该两次遍历的数据 提取为一层
//    rdd.flatMap(row=>row.split(",")).foreach(println)
    //过滤条件为真
//    val rddFilter = rdd.filter(row=>(row.split(",")(1)=="2013"))
//    rddFilter.foreach(println)
    //reduceByKey(
    val rdd3 = rdd.map(row=>(row.split(",")(1),row.split(",")(2).toInt))
//    val sum = rdd3.reduceByKey(_+_)
//    sum.foreach(println)
    //countByKey()
//    rdd3.countByKey().foreach(println)
    //take
    rdd.take(10).foreach(println)
    //分区 并行处理 提高效率 必须为key-value类型RDD才能分区
//    val rdd_k_v = rdd.map(row=>(row,""))
//    val rdd_repartition = rdd_k_v.partitionBy(new MySparkPartitioner(10))
//    rdd_repartition.saveAsTextFile("./test")
//    rdd_repartition.foreachPartition(partition=>{
//      println("----------------------------")
//      partition.foreach(row=>{
//        println(row._1.split(",")(1))
//      })
//    })
//    //缩减分区数量至指定数量 常用于多分区变为少分区
//    val rdd_5 = rdd_repartition.coalesce(5)
//    rdd_5.saveAsTextFile("./coalesce")
    //RDD持久化
    rdd3.persist(StorageLevel.MEMORY_ONLY_SER)
//    rdd3.unpersist(true)
    //广播变量
    val bc = sc.broadcast(rdd3.count)
    println(bc.value)
  }
  def initSparkContext(): SparkContext = {
    val conf: SparkConf = new SparkConf()
      .setAppName("sparkCore") //设置本程序名称
      .setMaster("local[*]") //启动本地 / 或远程计算
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("./checkpoints")
    sc
  }
  def readFile(): RDD[String] = {
    val fileContext = sc.textFile("sparkCore/src/main/resources/beijingpm.csv")
    fileContext
  }
  class  MySparkPartitioner(numParts: Int) extends Partitioner {
      override def numPartitions: Int = numParts
      /**
       * 可以自定义分区算法
       * @param key
       * @return
       */
      override def getPartition(key: Any): Int = {
        val key_to_str = key.toString
        val partition_fields = key_to_str.split(",")(1)
        partition_fields match {
          case null => 0
          case _ => partition_fields.hashCode%numPartitions
        }
      }
      override def equals(other: Any): Boolean = other match {
        case myPartition: MySparkPartitioner =>
          myPartition.numPartitions == numPartitions
        case _ =>
          false
      }
      override def hashCode: Int = numPartitions

    /**
     * def numPartitions：这个方法需要返回你想要创建分区的个数；
     * def getPartition：这个函数需要对输入的key做计算，然后返回该key的分区ID，范围一定是0到numPartitions-1；
     * equals()：这个是Java标准的判断相等的函数，之所以要求用户实现这个函数是因为Spark内部会比较两个RDD的分区是否一样。
     */
  }
}
