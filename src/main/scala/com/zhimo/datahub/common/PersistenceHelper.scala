package com.zhimo.datahub.common

import java.sql.Date
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by yaning on 5/2/18.
  */
object PersistenceHelper {
  val config = new ConfigHelper(this)

  def getParquetStorage(hiveStorage: String): String = {
    "tmp/" + hiveStorage + ".parquet"
  }

  def saveToParquetStorage(dataFrame: DataFrame, file: String, partitionBy: String = null, overwrite: Boolean = false): Unit = {
    val writer = dataFrame
      .write
      .format("parquet")
    val writerPartitioned = if (partitionBy == null) writer else writer.partitionBy(partitionBy)
    val writerMode = if (overwrite) writerPartitioned.mode("overwrite") else writerPartitioned.mode("append")
    writerMode.save(file)
    if(config.getBoolean("verifySave")) {
      val spark = SparkSessionSingleton.getInstanceIfExists()
      if (spark != null) {
        spark.sql("SELECT * FROM parquet.`" + file + "`").show(1000)
      }
    }
  }

  def saveToHive(dataFrame: DataFrame, table: String, partitionBy: String = null, overwrite: Boolean = false): Unit = {
    val writer = dataFrame
      .write
      .format("parquet")
    val writerPartitioned = if (partitionBy == null) writer else writer.partitionBy(partitionBy)
    val writerMode = if (overwrite) writerPartitioned.mode("overwrite") else writerPartitioned.mode("append")
    writerMode.saveAsTable(table)
  }

  def save(localEnvironment: Boolean, dataFrame: DataFrame, table: String, partitionBy: String = null, overwrite: Boolean = false): Unit = {
    if (localEnvironment) {
      saveToParquetStorage(dataFrame, getParquetStorage(table), null, overwrite)
    } else {
      saveToHive(dataFrame, table, partitionBy, overwrite)
    }
  }

  def saveAndShow(localEnvironment: Boolean, showResults:Boolean, dataFrame: DataFrame, table: String, partitionBy: String = null, overwrite: Boolean = false): Unit = {
    val toShow = if(showResults) dataFrame.persist() else dataFrame
    if(showResults) {
      toShow.show(10000)
    }
    save(localEnvironment,toShow,table,partitionBy,overwrite)
  }

  def delete(localEnvironment:Boolean, table:String):Unit= {
    val file = if(localEnvironment){
      getParquetStorage(table)
    }else{
      table
    }

    val spark = SparkSessionSingleton.getInstanceIfExists()
    if(localEnvironment){
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      hdfs.delete(new org.apache.hadoop.fs.Path(file), true)
    }else {

      if (spark != null) {
        spark.sql("DROP TABLE IF EXISTS default." + file)
      }
    }
  }

  def load(localEnvironment: Boolean, spark: SparkSession, table: String, operator:String=null,fromDate:Date=null): DataFrame = {
    if(localEnvironment) {
      if(operator!=null && fromDate != null)
        spark.read.parquet(getParquetStorage(table)).filter(s"(date$operator$fromDate)")
      else
        spark.read.parquet(getParquetStorage(table))
    }else{
      if(operator!=null && fromDate != null)
        spark.read.table(table).filter(s"(date$operator$fromDate)")
      else
        spark.read.table(table)
    }
  }

  def loadFromMysql(localEnvironment: Boolean,spark: SparkSession, table: String): DataFrame = {
    var mysqlUser,mysqlPass,mysqlServer = ""
    if(localEnvironment){
      mysqlUser = config.getString("mysql.user_dev")
      mysqlPass = config.getString("mysql.pass_dev")
      mysqlServer = config.getString("mysql.server_dev")
    }
    else{
      mysqlUser = config.getString("mysql.user_prod")
      mysqlPass = config.getString("mysql.pass_prod")
      mysqlServer = config.getString("mysql.server_prod")
    }
    val connectionProperties = new Properties()
    connectionProperties.put("user", mysqlUser)
    connectionProperties.put("password", mysqlPass)
    spark.read.jdbc(mysqlServer,table,connectionProperties)
  }
}
