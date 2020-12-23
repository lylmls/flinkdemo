package com.flink.demo.source

import java.util
import java.util.Properties

import com.flink.demo.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, RichFlatMapFunction, RuntimeContext}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests
/**
  * Author: ly
  * Date: 2020/12/23 0023 10:05
  */
object ReadTest {
  def main(args: Array[String]): Unit = {
/*    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hx08:9092,hx07:9092,hx10:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "earliest ")*/
//    properties.setProperty("auto.offset.reset", "latest")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
/*    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("topic_create", new SimpleStringSchema(), properties))
    stream3.map(x => println(x)).print()
    env.execute("job_k")*/


    val stream2 = env.readTextFile("E:\\work_path_oth\\flinkdemo\\src\\main\\resources\\data.txt")
      .map( data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
/*      .keyBy("id")
      .reduce( (x, y) => {
        println(x.timeStamp)
        SensorReading(x.id, x.timeStamp + 1, if (y.temperature > x.temperature) y.temperature else x.temperature )
      })*/
    val splitStream  = stream2.split(senserData => if(senserData.temperature > 35.5) Seq("high") else Seq("low"))
    val highStream = splitStream.select("low")
      //.map(_.temperature.toString)
//    highStream.addSink(new FlinkKafkaProducer011[String]("hx08:9092,hx07:9092,hx10:9092","test", new SimpleStringSchema()))
/*    val fl = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9)
    fl.flatMap(new MyFlatMap).print("ip")*/

/*    val conf = new FlinkJedisPoolConfig.Builder().setHost("hx08").setPort(6379).build()
    highStream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper))*/

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hx08", 9200))
    val esSinkBuilder  = new ElasticsearchSink.Builder[SensorReading](httpHosts, new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        println("saving data: " + t)
        val json = new util.HashMap[String, String]()
        json.put("data", t.toString)
        val indexRequest = Requests.indexRequest().index("sensor").`type`("readingData").source(json)
        requestIndexer.add(indexRequest)
        println("saved successfully")
      }
    })
    highStream.addSink(esSinkBuilder.build())

    env.execute()

  }


}

class MyRedisMapper extends RedisMapper[SensorReading]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }

  override def getKeyFromData(t: SensorReading): String = {
    t.id
  }

  override def getValueFromData(t: SensorReading): String = {
    t.temperature.toString
  }
}

class KeyWordFilter(keyWord: String) extends FilterFunction[String]{
  override def filter(value: String): Boolean = {
    value.contains(keyWord)
  }
}

class MyFlatMap extends RichFlatMapFunction[Int, (Int, Int)]{
  var subTaskIndex = 0
  override def open(parameters: Configuration): Unit = {
    subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
  }

  override def flatMap(in: Int, out: Collector[(Int, Int)]): Unit = {
    if (in % 2 == subTaskIndex) {
      out.collect((subTaskIndex, in))
    }
  }
}
