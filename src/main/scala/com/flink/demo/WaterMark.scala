package com.flink.demo

import java.util
import java.util.Properties

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object WaterMark {
  def main(args: Array[String]): Unit = {
    waterDemo
  }

  def waterDemo() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dstream = env.socketTextStream("10.1.2.59", 7777)
    val vStream = dstream.map(x => {
      val sp = x.split(",")
      new SensorReading(sp(0), sp(1).toLong, sp(2).toDouble)
      //      print(sp)
    }).keyBy(_.id)
      .process(new TempIncreaseAlertFunction)

    vStream.print()

    env.execute()
  }


}

class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SensorReading, String] {
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("lastTemp", Types.of[Double])
  )
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("timer", Types.of[Long])
  )

  override def processElement(i: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    val prevTemp = lastTemp.value()
    lastTemp.update(i.temperature)
    val curTimerTimestamp = currentTimer.value()
    //温度下降或者是第一个温度值，删除定时器
    if (prevTemp == 0 || i.temperature < prevTemp) {
      ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
      currentTimer.clear()
    } else if (i.temperature > prevTemp && curTimerTimestamp == 0) {
      // 温度升高
      val timerTs = ctx.timerService().currentProcessingTime() + 10000
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      currentTimer.update(timerTs)
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    //    super.onTimer(timestamp, ctx, out)
    out.collect("传感器id为: " + ctx.getCurrentKey + "的传感器温度值已经连续1s上升了。")
    currentTimer.clear()
  }
}
