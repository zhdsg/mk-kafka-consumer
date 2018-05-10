
import org.apache.spark.sql.DataFrame

/**
  * Created by yaning on 5/2/18.
  */
object PersistenceHelper {

  def saveToParquetStorage(dataFrame: DataFrame, file: String, overwrite: Boolean = false): Unit = {
    val writer = dataFrame
      .write
      .format("parquet")
      .partitionBy("date")
    val writerMode = if (overwrite) writer.mode("overwrite") else writer.mode("append")
    writerMode.save(file)
  }
}
