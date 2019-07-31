package cn.itdeer.flink.demo02

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
  * Created by Administrator on 2019/7/24.
  */
object ScalaDataSourceApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    //    fromCollection(env)
    //    fromTextFile(env)
    //    fromCSV(env)

//          fromTextFiles(env)
    formCompressedFile(env)
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
    * DataSource 为文件或者单层目录
    *
    * @param env
    */
  def fromTextFile(env: ExecutionEnvironment): Unit = {
    val path1 = "F:\\Code\\Study\\FlinkProject\\src\\main\\scala\\cn\\itdeer\\scaladata\\p2"
    env.readTextFile(path1).print()

    val path2 = "F:\\Code\\Study\\FlinkProject\\src\\main\\scala\\cn\\itdeer\\scaladata\\inputs"
    env.readTextFile(path2).print()
  }

  /**
    * DataSource 为CSV文件
    * @param env
    */
  def fromCSV(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val path = "F:\\Code\\Study\\FlinkProject\\src\\main\\scala\\cn\\itdeer\\scaladata\\data02.csv"
    //    env.readCsvFile[(String, Integer, String)](path, ignoreFirstLine = true).print()
    //    env.readCsvFile[(String, Integer)](path, ignoreFirstLine = true,includedFields = Array(0,1)).print()
    //    env.readCsvFile[(String, Integer)](path, ignoreFirstLine = true,includedFields = Array(0,1)).print()

    case class MyCaseClass(name: String, age: Double)
    env.readCsvFile[MyCaseClass](path, ignoreFirstLine = true, includedFields = Array(0, 1)).print()

    //      env.readCsvFile[Person](path, ignoreFirstLine = true,pojoFields = Array("name","age","address")).print()
  }

  /**
    * DataSource 为文件,多层目录
    * @param env
    */
  def fromTextFiles(env: ExecutionEnvironment): Unit ={
    val path = "F:\\Code\\Study\\FlinkProject\\src\\main\\scala\\cn\\itdeer\\scaladata\\inputs"

    val parameters = new Configuration
    parameters.setBoolean("recursive.file.enumeration", true)
    env.readTextFile(path).withParameters(parameters).print()
  }

  /**
    * DataSource 为压缩文件
    * @param env
    */
  def formCompressedFile(env: ExecutionEnvironment): Unit ={

    val path = "F:\\Code\\Study\\FlinkProject\\src\\main\\scala\\cn\\itdeer\\scaladata\\data03.gz"

    env.readTextFile(path).print()

  }

}
