package com.zhimo.datahub.common

import kafka.common.TopicAndPartition
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange


import org.apache.spark.rdd.MapPartitionsRDD
import scala.collection.mutable


/**
 * Created by Administrator on 2018/11/26/026.
 */
class OffsetManager extends Serializable{

  def readOffset(topics : Array[String],config: ConfigHelper):  Option[Map[TopicPartition,Long]] ={
    val offsetTab =config.getString("kafka.mysql.topicTab")
    val localDev = config.getBoolean("localDev")
    var topicMysqlUrl,topicMysqlUser,topicMysqlPasswd = ""
    if(localDev ){
       topicMysqlUrl = config.getString("kafka.mysql.server_dev")
       topicMysqlUser = config.getString("kafka.mysql.user_dev")
       topicMysqlPasswd = config.getString("kafka.mysql.pass_dev")
    }else{
      topicMysqlUrl = config.getString("kafka.mysql.server_prod")
      topicMysqlUser = config.getString("kafka.mysql.user_prod")
      topicMysqlPasswd = config.getString("kafka.mysql.pass_prod")
    }


    val sql =new StringBuilder(s"select * from  ${offsetTab} ")
    for( i  <- 0 until topics.size ) {
      if (i == 0) {
        sql.append(s" where topic in ( '${topics(i)}' ")
      } else{
        sql.append(s",'${topics(i)}' ")
      }

      if(i== topics.size-1){
        sql.append(")")
      }
    }
    val map = new mutable.HashMap[TopicPartition,Long]

    val jdbc =JDBCUtil (topicMysqlUrl,topicMysqlUser,topicMysqlPasswd)
    val rs =jdbc.executeQuery(sql.toString())
    while (rs.next()){
      map.+=((new TopicPartition(rs.getString("topic"),rs.getInt("partition")),rs.getLong("offset")))
    }
    jdbc.close()

   if (map.size>0) Some(map.toMap) else None

  }

  def writeOffset(offsetRange:Array[OffsetRange],config: ConfigHelper,start:Boolean)={
    val offsetTab =config.getString("kafka.mysql.topicTab")
    val localDev = config.getBoolean("localDev")
    var topicMysqlUrl,topicMysqlUser,topicMysqlPasswd = ""
    if(localDev ){
      topicMysqlUrl = config.getString("kafka.mysql.server_dev")
      topicMysqlUser = config.getString("kafka.mysql.user_dev")
      topicMysqlPasswd = config.getString("kafka.mysql.pass_dev")
    }else{
      topicMysqlUrl = config.getString("kafka.mysql.server_prod")
      topicMysqlUser = config.getString("kafka.mysql.user_prod")
      topicMysqlPasswd = config.getString("kafka.mysql.pass_prod")
    }

    val sql =new StringBuilder(s"replace into ${offsetTab} values " )
    for(i <- 0 until offsetRange.length ) {
      if (i == 0) {
        sql.append(s" ( '${offsetRange(i).topic}' ,'${offsetRange(i).partition}','${if(start) offsetRange(i).fromOffset else offsetRange(i).untilOffset}' ) ")
      } else{
        sql.append(s" , ( '${offsetRange(i).topic}' ,'${offsetRange(i).partition}','${if(start) offsetRange(i).fromOffset else offsetRange(i).untilOffset}' ) ")
      }

    }
    val jdbc =JDBCUtil (topicMysqlUrl,topicMysqlUser,topicMysqlPasswd)
    jdbc.executeUpdate(sql.toString())
    jdbc.close()


  }

}
object OffsetManager{
  def main(args: Array[String]) {
    val config = new ConfigHelper(this)
//    new OffsetManager().readOffset(Array("aaa","bbb"),config) match {
//      case Some(a) => a.foreach(f=>println(f._1+"|"+f._2))
//      case None=> println("null")
//    }
    val array = Array(OffsetRange.create("aaa",1,1111,3),OffsetRange.create("bbb",2,1111,4),OffsetRange.create("ddd",222,1111,2222))
    new OffsetManager().writeOffset(array,config,true)

  }
}
