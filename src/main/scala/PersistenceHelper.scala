import java.util.Properties

import com.typesafe.config.ConfigFactory
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

  def saveToParquetStorage(dataFrame: DataFrame, file: String, partitionBy:String = null, overwrite: Boolean = false): Unit = {
    val writer = dataFrame
      .write
      .format("parquet")
    val writerPartitioned = if(partitionBy == null) writer else writer.partitionBy(partitionBy)
    val writerMode = if (overwrite) writerPartitioned.mode("overwrite") else writerPartitioned.mode("append")
    writerMode.save(file)
  }
}
