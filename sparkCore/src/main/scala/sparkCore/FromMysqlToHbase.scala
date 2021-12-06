package sparkCore

import CommonUtils.{ConnectPropertis, HBaseUtils, TableFieldNames}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, functions}
import org.apache.spark.storage.StorageLevel

import java.net.URI
import scala.collection.immutable.TreeMap

object FromMysqlToHbase {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")

    val id_RowKey = "id"
    val columnFamily = "info"
    val tableName = "tbl_logs"
    val tableFieldNames = TableFieldNames.LOG_FIELD_NAMES
    val sparkSession = ConnectPropertis.spark
    val df = readMysql(tableName)
    to_SmallPieces(df,id_RowKey,columnFamily,tableName,tableFieldNames)
    df.unpersist()
    sparkSession.close()
  }
  //
  // 01 读取mysql数据表 -> DataFrame
  def readMysql(tableName:String): DataFrame ={
    val properties = ConnectPropertis.properties;val uri = ConnectPropertis.mysql_uri
    val mysqlDataSource = ConnectPropertis.spark.read.jdbc(url = uri,table=tableName,properties = properties)
    mysqlDataSource.persist(StorageLevel.MEMORY_AND_DISK)
    mysqlDataSource.count()
    mysqlDataSource
  }
  // 02 将mysql数据表截成小段处理 DataFrame -> slice DataFrame
  def to_SmallPieces(dataFrame: DataFrame, id_RowKey:String, columnFamily:String, tableName: String,tableFields:TreeMap[String,Int]): Unit ={
    val maxId = dataFrame.select(functions.max(id_RowKey)).take(1)(0)(0)
    var point = Integer.parseInt(dataFrame.select(functions.min(id_RowKey)).take(1)(0)(0).toString)
    while (point<Integer.parseInt(maxId.toString)){
      val df = dataFrame.select("*")
        .where("%s>=%d".format(id_RowKey,point))
        .limit(30000)
      point = Integer.parseInt(df.select(functions.max(id_RowKey)).take(1)(0)(0).toString)
      //将每两万条记录写为HFile文件
      val rdd = toHbaseLine(df,columnFamily,tableFields)
      prepareHbaseEnv(rdd,tableName)
//      println(point)
//      df.show(3)
//      if (point>1000) {breakOut}
    }
  }
  // 04 ROW ->KeyValue 将DataFrame的每行处理为 (writeable,keyValue)
  def getLineData(row:Row,columnFamily:String,tableFields:TreeMap[String,Int]): List[(ImmutableBytesWritable, KeyValue)] ={
    val rowKey = Bytes.toBytes(row(0).toString)
    val writeable = new ImmutableBytesWritable(rowKey)
    val cf = Bytes.toBytes(columnFamily)
    tableFields.toList.map{
      case (fieldName, fieldsIndex) =>
        val keyValue = new KeyValue(
          rowKey,
          cf,
          Bytes.toBytes(fieldName),
          if (row(fieldsIndex)==null){
            Bytes.toBytes("")
          }else{
            Bytes.toBytes(row(fieldsIndex).toString)
          }
        )
        (writeable,keyValue)
    }
  }
  // 03 将DataFrame -> RDD
  def toHbaseLine(dataFrame: DataFrame,columnFamily:String,tableFieldNames: TreeMap[String, Int]): RDD[(ImmutableBytesWritable,KeyValue)] ={
    val rdd:RDD[(ImmutableBytesWritable,KeyValue)] = dataFrame.rdd.flatMap(
      x=>getLineData(x, columnFamily, tableFieldNames)
    )
      .sortByKey()
    rdd.persist(StorageLevel.DISK_ONLY)
    rdd.count()
    rdd
  }
  // 05 输出到HBASE
  def prepareHbaseEnv(rdd:RDD[(ImmutableBytesWritable,KeyValue)],tableName:String): Unit ={
    // 如果hdfs输出目录存在则删除
    val conf: Configuration = new Configuration
    val fs: FileSystem = FileSystem.get(URI.create("hdfs://ns1"), conf, "root")
    val outputPath: Path = new Path("hdfs://ns1/user/root/hbase/tempHFile/"+tableName)
    if (fs.exists(outputPath)) {fs.delete(outputPath, true)}
    fs.close()
    // zookeeper地址
    val hbaseConfig: Configuration = HBaseUtils.getHBaseConfiguration("ha002,ha001,ha003", "2181",tableName)
    // 输出表 一定要配置
    hbaseConfig.set("hbase.mapreduce.hfileoutputformat.table.name", tableName)
    // 配置HFileOutputFormat2输出
    val conn = ConnectionFactory.createConnection(hbaseConfig)
    val htableName = TableName.valueOf(tableName)
    val table: Table = conn.getTable(htableName)
    HFileOutputFormat2.configureIncrementalLoad(
      Job.getInstance(hbaseConfig), //
      table, //
      conn.getRegionLocator(htableName) //
    )
    // 保存数据为HFile文件   先排序
    rdd.sortBy(x => (x._1, x._2.getKeyString), ascending = true)
      .saveAsNewAPIHadoopFile(
        outputPath.toString,
        classOf[ImmutableBytesWritable], //
        classOf[KeyValue], //
        classOf[HFileOutputFormat2], //
        hbaseConfig)
    // 将输出HFile加载到HBase表中
    val load = new LoadIncrementalHFiles(hbaseConfig)
    load.doBulkLoad(outputPath, conn.getAdmin, table,
      conn.getRegionLocator(htableName))
    conn.close()
  }

}
