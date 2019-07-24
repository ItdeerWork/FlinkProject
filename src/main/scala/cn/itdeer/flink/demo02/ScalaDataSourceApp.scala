package cn.itdeer.flink.demo02

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Created by Administrator on 2019/7/24.
  */
object ScalaDataSourceApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

//    fromCollection(env)
//    fromTextFile(env)
    fromCSV(env)
  }

  /**
    * DataSource 为集合
    *
    * @param env
    */
  def fromCollection(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._

    val data = 1 to 10
    env.fromCollection(data).print()
  }

  /**
    * DataSource 为文件
    *
    * @param env
    */
  def fromTextFile(env: ExecutionEnvironment): Unit = {
    val path1 = "F:\\Code\\Study\\FlinkProject\\src\\main\\scala\\cn\\itdeer\\scaladata\\data01"
    env.readTextFile(path1).print()

    val path2 = "F:\\Code\\Study\\FlinkProject\\src\\main\\scala\\cn\\itdeer\\scaladata\\inputs"
    env.readTextFile(path2).print()
  }

  def fromCSV(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val path = "F:\\Code\\Study\\FlinkProject\\src\\main\\scala\\cn\\itdeer\\scaladata\\data02.csv"
    env.readCsvFile[(String, Integer, String)](path, ignoreFirstLine = true).print()
  }

}
