package cn.itdeer.flink.demo02

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

/**
  * Description : 
  * PackageName : cn.itdeer.flink.demo02
  * ProjectName : FlinkProject
  * CreatorName : itdeer.cn
  * CreateTime : 2019/8/1/16:41
  */
object ScalaTransformationsApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

//    mapFunction(env)
//    filterFunction(env)
    mapPartitionFunction(env)
  }

  /**
    * Map 函数
    * @param env
    */
  def mapFunction(env : ExecutionEnvironment): Unit ={

    val data = env.fromCollection(List(1,2,3,4,5,6,7,8,9,10))

//    data.print()

//    data.map((x : Int) => x + 1).print()

//    data.map((x) => x + 1).print()

//    data.map(x => x + 1).print()

    data.map(_ + 1).print()
  }

  /**
    * Filter 函数
    * @param env
    */
  def filterFunction(env : ExecutionEnvironment): Unit ={
    val data = env.fromCollection(List(1,2,3,4,5,6,7,8,9,10))

    data.map(_ * 2).filter(_ > 5).print()
  }

  /**
    * mapPartition 函数
    * @param env
    */
  def mapPartitionFunction(env : ExecutionEnvironment): Unit ={

    val students = new ListBuffer[String]

    for(i <- 1 to 100){
      students.append("students: " + i)
    }

    val data = env.fromCollection(students).setParallelism(5)


//    data.map( x =>{
//      val connection= DBUtils.getConnection
//      println(connection + "...")
//      DBUtils.returnConnection(connection)
//    }).print()

    data.mapPartition(x => {
      val connection= DBUtils.getConnection
      println(connection + "...")
      DBUtils.returnConnection(connection)
      x
    }).print()

  }

}