package cn.itdeer.flink.demo01

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Description : 
  * PackageName : cn.itdeer.flink
  * ProjectName : FlinkProject
  * CreatorName : itdeer.cn
  * CreateTime : 2019/7/25/15:09
  */
object ScalaStreamingWCApp {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", 9999)

    import org.apache.flink.api.scala._

    text.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .print()
      .setParallelism(1)

    env.execute("StreamingWCScalaApp")
  }
}