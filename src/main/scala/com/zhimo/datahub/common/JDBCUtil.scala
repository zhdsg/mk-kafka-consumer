package com.zhimo.datahub.common

import java.sql.{ResultSet, PreparedStatement, DriverManager, Connection}
import java.util


/**
 * Created by Administrator on 2018/11/26/026.
 */
class JDBCUtil(input_url:String , input_user:String ,input_passwd:String){

  var url: String =input_url
  var user: String =input_user
  var passwd :String =input_passwd

  var conn: Connection =null

  def initConn(): Unit ={
    val driver = "com.mysql.jdbc.Driver"
    Class.forName(driver)

    conn = DriverManager.getConnection(url,user,passwd)

  }

  def executeQuery(sql:String  ): ResultSet ={
    if(conn ==null){
      initConn()
    }
    val ps: PreparedStatement =  conn.prepareStatement(sql)
    ps.executeQuery()

  }

  def executeUpdate(sql :String ) :Int={
    if(conn ==null){
      initConn()
    }
    val ps:PreparedStatement =conn.prepareStatement(sql)
    ps.executeUpdate()

  }
  def close (): Unit ={
    conn.close()
  }
}
object  JDBCUtil{

  def apply(url:String ,user:String,passwd:String ) = new JDBCUtil(url,user,passwd)

}