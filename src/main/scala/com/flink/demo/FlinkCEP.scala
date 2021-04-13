package com.flink.demo

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

import java.util


/**
  * Author: ly
  * Date: 2020/12/23 0023 10:02
  */

case class OrderEvent(orderId: Long, eventType:String,txId: String, ts: Long)

case class OrderResult(orderId: Long, resultMsg: String)

object FlinkCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1、从文件中读取数据
    val resource = getClass.getResource("/OrderLog.csv")
    val orderEvnetStream = env.readTextFile(resource.getPath)
      .map(d=>{
        val arr = d.split(",")
        OrderEvent(arr(0).toLong,arr(1),arr(2), arr(3).toLong)  //把数据读出来转换成想要的样例类类型
      }).assignAscendingTimestamps(_.ts * 1000)  //指定ts字段
      .keyBy(_.orderId) //按照订单id分组



    /**
      * 2、定义事件-匹配模式
      *  定义15分钟内能发现订单创建和支付
      */
    val orderPayPattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")  //先出现一个订单创建的事件
      .followedBy("pay").where(_.eventType == "pay")            //后边再出来一个支付事件
      .within(Time.seconds(5))                                //定义在15分钟以内，触发这2个事件

    // 3、将pattern应用到流里面，进行模式检测
    val patternStream = CEP.pattern(orderEvnetStream, orderPayPattern)

    //4、定义一个侧输出流标签，用于处理超时事件
    val orderTimeoutTag = new OutputTag[OrderResult]("orderTimeout")

    // 5、调用select 方法，提取并处理匹配的成功字符事件以及超时事件
    val resultStream = patternStream.select(
      orderTimeoutTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect()
    )

    resultStream.print("pay")
    resultStream.getSideOutput(orderTimeoutTag).print()
    env.execute(" order timeout monitor")

  }
}


//获取超时之后定义的事件还没触发的情况，也就是订单支付超时了。
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderId = map.get("create").iterator().next().orderId
    OrderResult(timeoutOrderId, "超时了。。。。超时时间："+l)
  }
}

class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {

    val orderTs = map.get("create").iterator().next().ts
    val paydTs = map.get("pay").iterator().next().ts
    val payedOrderId = map.get("pay").iterator().next().orderId
    OrderResult(payedOrderId, "订单支付成功，下单时间:"+orderTs+" 支付时间："+paydTs)
  }
}







