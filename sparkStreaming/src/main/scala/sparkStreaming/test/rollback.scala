package sparkStreaming.test

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ArrayBuffer

object rollback{
  var orders: Int = 0
  var huiyuan:Int = 0
  var now:Long = new Date().getTime
  def getNowDate:ArrayBuffer[Int]={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("SS")
    var hehe = dateFormat.format( now )
    print(hehe)
    if (hehe == "59"){
      orders=3
      huiyuan=3
    }
    orders+=1
    huiyuan+=2
    val b = ArrayBuffer[Int]()
    b += (orders,huiyuan)
    b
  }
  def periodicCall(s : Integer,callback:()=>Unit):Unit={
    while(true){
      callback()
      Thread.sleep(s*1000)
    }
  }

    def main(args: Array[String]): Unit = {
      val now_minite: ArrayBuffer[Int] = getNowDate
      print(now_minite)
      //采用匿名函数调用
      periodicCall(2, ()=>print(now))
    }

}