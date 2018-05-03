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

  def saveToParquetStorage(dataFrame: DataFrame, file: String, overwrite: Boolean = false): Unit = {
    val writer = dataFrame
      .write
      .format("parquet")
      .partitionBy("date")
    val writerMode = if (overwrite) writer.mode("overwrite") else writer.mode("append")
    writerMode.save(file)
  }
}
