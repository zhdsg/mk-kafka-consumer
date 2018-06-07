import org.apache.spark.sql.SparkSession
import java.util.Properties

import org.apache.spark.sql.DataFrame

/**
  * Created by yaning on 5/2/18.
  */
object PersistenceHelper {
  val config = new ConfigHelper(this)
  val mysqlUser = config.getString("mysql.user")
  val mysqlPass = config.getString("mysql.pass")
  val mysqlServer = config.getString("mysql.server")
  val mysqlServerLogTable = config.getString("mysql.serverLogTable")
  val connectionProperties = new Properties()
  connectionProperties.put("user", mysqlUser)
  connectionProperties.put("password", mysqlPass)

  def getParquetStorage(hiveStorage:String):String = {
    "tmp/" + hiveStorage + ".parquet"
  }

  def saveToParquetStorage(dataFrame: DataFrame, file: String, partitionBy:String = null, overwrite: Boolean = false): Unit = {
    val writer = dataFrame
      .write
      .format("parquet")
    val writerPartitioned = if(partitionBy == null) writer else writer.partitionBy(partitionBy)
    val writerMode = if (overwrite) writerPartitioned.mode("overwrite") else writerPartitioned.mode("append")
    writerMode.save(file)
  }

  def saveToHive(dataFrame: DataFrame, table: String, partitionBy:String = null, overwrite: Boolean = false): Unit = {
    val writer = dataFrame
      .write
      .format("parquet")
    val writerPartitioned = if(partitionBy == null) writer else writer.partitionBy(partitionBy)
    val writerMode = if (overwrite) writerPartitioned.mode("overwrite") else writerPartitioned.mode("append")
    writerMode.saveAsTable(table)
  }

  def save(localEnvironment:Boolean,dataFrame: DataFrame, table: String, partitionBy:String = null, overwrite: Boolean = false): Unit =
  {
    if(localEnvironment){
      saveToParquetStorage(dataFrame,getParquetStorage(table),partitionBy,overwrite)
    }else{
      saveToHive(dataFrame,table,partitionBy,overwrite)
    }
  }

  def load(localEnvironment:Boolean,spark:SparkSession,table:String): DataFrame ={
    spark.read.parquet(getParquetStorage(table))
  }
}
