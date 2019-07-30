package cn.itdeer.flink.demo01

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Description : 
  * PackageName : cn.itdeer.flink.demo01
  * ProjectName : FlinkProject
  * CreatorName : itdeer.cn
  * CreateTime : 2019/7/25/15:07
  */
object ScalaBatchWCApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val path = "F:\\Code\\Flink\\flink-train-java\\src\\main\\java\\cn\\itdeer\\flink\\java\\demo1\\data1"

    val text = env.readTextFile(path)

    import org.apache.flink.api.scala._

    text.flatMap(_.toLowerCase.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()
  }
}
