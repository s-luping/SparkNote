package sparkSql

import CommonUtils.ConnectPropertis
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object SparkSQL {

  def main(args: Array[String]): Unit = {
    val read = new readDataSourceSQL.readStructData
    read.readHive()
//    val filePath = "file://G:/mytaks/hadoop/spark/SparkLearn/src/main/resources/data/sparktest/beijingpm.csv"
//    //读取成rdd
//    val rdd = sc.textFile(filePath).map(_.split(" "))
//    //1. 使用toDF  隐式转换
//    import spark.implicits._
//    val dataFrame = rdd.map(x=>(x(0).toInt,x(1),x(2),x(3).toInt))
//     .toDF("id":String,"province":String,"sex":String,"height":String)
    //(1)查询所有记录的ID、性别和身高;
      //dataFrame.select("id","sex","height").show()
    //(2)查询统计身高在160-180的性别为M的相关人员
      //dataFrame.select("*").where("160 <=cast(height as int) and cast(height as int)<=180 and sex='M'").show()
    //(3)查询统计hebeiprovince省的男性人员信息，并按照身高降序排序;
      //dataFrame.select("*").where("province='hebeiprovice' and sex='M'").orderBy(dataFrame("height").desc).show()
    //(4)查询hebeiprovince省的人员数目;
      //println(dataFrame.where("province='hebeiprovice'").count())
    //(5)查询统计不同省份的平均身高是多少;
      //dataFrame.groupBy("province").mean("height").show()
    // 四、spark+mysql+hive整合sparksql完成如下要求:(20分)
  }
}
