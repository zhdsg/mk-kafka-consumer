import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame

/**
  * Created by yaning on 5/2/18.
  */
object PersistenceHelper {
  val config = ConfigFactory.load()
  val mysqlUser = config.getString("environment.mysqlUser")
  val mysqlPass = config.getString("environment.mysqlPass")
  val mysqlServer = config.getString("environment.mysqlServer")
  val mysqlServerLogTable = config.getString("environment.mysqlServerLogTable")
  val connectionProperties = new Properties()
  connectionProperties.put("user", mysqlUser)
  connectionProperties.put("password", mysqlPass)

  def saveToParquetStorage(dataFrame: DataFrame, file: String, partitionBy:String = null, overwrite: Boolean = false): Unit = {
    val writer = dataFrame
      .write
      .format("parquet")
    val writerPartitioned = if(partitionBy == null) writer else writer.partitionBy(partitionBy)
    val writerMode = if (overwrite) writerPartitioned.mode("overwrite") else writerPartitioned.mode("append")
    writerMode.save(file)
  }
  def saveToMysql(dataFrame: DataFrame,overwrite:Boolean=false):Unit={
    val writerMode = if (overwrite) "overwrite" else "append"
    dataFrame.write.mode(writerMode)
      .jdbc(mysqlServer, mysqlServerLogTable, connectionProperties)
  }
}
