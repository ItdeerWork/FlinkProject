package cn.itdeer.flink.demo02

import scala.util.Random

/**
  * Description : 
  * PackageName : cn.itdeer.flink.demo02
  * ProjectName : FlinkProject
  * CreatorName : itdeer.cn
  * CreateTime : 2019/8/1/17:37
  */
object DBUtils {

  def getConnection = {
    new Random().nextInt(10) + ""
  }

  def returnConnection(connection : String) = {

  }

}