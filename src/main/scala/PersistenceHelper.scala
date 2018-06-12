import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.DataFrame

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
        spark.sql("DROP TABLE IF EXISTS parquet.`" + file + "`")
      }
    }
  }

  def load(localEnvironment: Boolean, spark: SparkSession, table: String): DataFrame = {
    spark.read.parquet(getParquetStorage(table))
  }
}
