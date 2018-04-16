/**
  * Created by yaning on 4/11/18.
  */
import java.sql.Timestamp
import java.util.Properties

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

// Create a local StreamingContext with two working thread and batch interval of 1 second.
// The master requires 2 cores to prevent a starvation scenario.


object SparkTest extends Logging  {
  def main(args: Array[String]): Unit ={
    val connectionProperties = new Properties()
    connectionProperties.put("user", "xubao")
    connectionProperties.put("password", "8xd8IiHR")
    val conf = new SparkConf().setMaster("local[2]").setAppName("sparkTest")
    val ssc = new StreamingContext(conf, Seconds(1))
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)

    lines.foreachRDD { rdd=>
      if(!rdd.partitions.isEmpty){
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        import spark.implicits._
        val structuredData = spark.read.option("multiline", true).option("mode", "PERMISSIVE").json(rdd.toDS()).as[Record]
        val df = structuredData.toDF() // create a dataframe from the schema RDD
        df.show()

        logInfo("about to write to mysql!")
        df.write.mode("append")
          .jdbc("jdbc:mysql://10.10.100.2:3306/erpstatsdev?useUnicode=true&characterEncoding=UTF-8", "MK_action", connectionProperties)
      }
      else{
        logInfo("got empty rdd!")
      }
    }
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
/** Case class for converting RDD to DataFrame */
case class Record(uid:String,e_c: String,e_a:String,e_n:String,e_v:String,t:Timestamp)


/** Lazily instantiated singleton instance of SparkSession */
object SparkSessionSingleton {

  @transient  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}