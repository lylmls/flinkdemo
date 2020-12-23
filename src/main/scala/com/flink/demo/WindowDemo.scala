package com.flink.demo

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
/**
  * Author: ly
  * Date: 2020/12/23 0023 16:21
  */
object WindowDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    timeWindow(env)

    env.execute("window")
  }

  def timeWindow(env: StreamExecutionEnvironment) ={
    val socDstream = env.socketTextStream("10.1.2.59", 7777)
    val minTempPerWindow  = socDstream.map(r => {
      val sp = r.split(",")
      if (sp.length != 3) {
        ("", "", 0)
      } else {
        (sp(0), sp(1), sp(2).toInt)
      }
    }).filter(_._1 != "")
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .reduce((r1, r2) => (r1._1, r1._2, r1._3.min(r2._3)))
    minTempPerWindow.print()
  }

  def slidingEventTimeWindow(env: StreamExecutionEnvironment) = {
    val socDstream = env.socketTextStream("10.1.2.59", 7777)
    val minTempPerWindow  = socDstream.map(r => {
      val sp = r.split(",")
      if (sp.length != 3) {
        ("", "", 0)
      } else {
        (sp(0), sp(1), sp(2).toInt)
      }
    }).filter(_._1 != "")
      .keyBy(_._1)
      .timeWindow(Time.seconds(15), Time.seconds(5))
      .reduce((r1, r2) => (r1._1, r1._2, r1._3.min(r2._3)))
    minTempPerWindow.print()
  }

}
