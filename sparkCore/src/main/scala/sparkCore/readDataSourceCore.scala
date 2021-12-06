package sparkCore

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import sparkCore.SparkCore.sc



object readDataSourceCore {
  def main(args: Array[String]): Unit = {
    val read = readStructureData()
    read.readHbase()
  }


  case class readStructureData(){
    def readData(): Unit ={
      val inputPath = "file:///G:\\mytaks\\hadoop\\spark\\SparkLearn\\src\\main\\resources\\data\\weibo\\weibo.json"
      val rdd1 = sc.textFile(inputPath)
      //      .repartition(3)
      //      .map(_.split("\\t")(0))
      //      .filter(x=>(!x.contains("127.0.0")))
      //      .map(w=>(w,1))
      //      .reduceByKey((a,b)=>a+b)
      //      .sortByKey(ascending = false)
      //    val rdd2 = rdd1.repartitionAndSortWithinPartitions(Partitioner.defaultPartitioner(rdd1))
      //    val rdd3 = rdd2.sample(withReplacement = false,.5,5)
      rdd1.foreach(println)
    }
    def readHdfs(): Unit ={
      val inputPath = "hdfs://hbase:9000/weblog/year=2021/month=06/day=15/15.log"
      val rdd1 = sc.textFile(inputPath)
      rdd1.foreach(println)
    }
    def readHbase(): Unit ={
      val hbaseConfig:Configuration = HBaseConfiguration.create() // 配置都封装成<k,v>
      hbaseConfig.set("hbase.zookeeper.quorum","ha001,ha002,ha003")
      hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
      hbaseConfig.set("maxSessionTimeout", "30000")
      hbaseConfig.set("hbase.rpc.timeout","30000")
      hbaseConfig.set("hbase.client.scanner.timeout.period","30000")
      hbaseConfig.set(TableInputFormat.INPUT_TABLE, "test")
      val hbaseRdd = sc.newAPIHadoopRDD(hbaseConfig,
        classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])
      val count = hbaseRdd.count()
      println("Students RDD Count:" + count)
      //遍历输出
      hbaseRdd.foreach({ case (_,result) =>
        val key = Bytes.toString(result.getRow)
        val name = Bytes.toString(result.getValue("info".getBytes,"name".getBytes))
        val age = Bytes.toString(result.getValue("info".getBytes,"age".getBytes))
        val gender = Bytes.toString(result.getValue("info".getBytes,"gender".getBytes))
        println("Row key:"+key+" name:"+name+" age"+age+" gender"+gender)
      })
    }
  }
}
