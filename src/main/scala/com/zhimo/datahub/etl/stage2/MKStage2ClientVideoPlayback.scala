package com.zhimo.datahub.etl.stage2

import java.sql.Date

import com.zhimo.datahub.common.{ConfigHelper, PersistenceHelper, SparkSessionSingleton}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{sum}

/**
  * Created by yaning on 6/28/18.
  */
object MKStage2ClientVideoPlayback extends Logging{
  def main(args: Array[String]) {
    val startTime = System.nanoTime()
    val config = new ConfigHelper(this)
    val processFromStart = config.getBoolean("processFromStart")
    val localDevEnv = config.getBoolean("localDev")

    // Template: Specify permanent storage Parquet file
    val storage = config.getEnvironmentString("storage.client")


    val sparkConf = new SparkConf()
      .setAppName("MKStage2ClientVideoPlayback")
    if (localDevEnv) {
      sparkConf.setMaster("local")
    } else {
      sparkConf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")
    }

    val spark = SparkSessionSingleton.getInstance(sparkConf, !localDevEnv)

    if (localDevEnv) {
      spark.sparkContext.setLogLevel("ERROR")
    }

    import spark.implicits._
    val rawRecords = PersistenceHelper.loadFromParquet(spark, storage)
    rawRecords.printSchema()
    rawRecords.show()
    val records =rawRecords.as[VideoPlaybackRaw].filter(log=>{
      log.e_c != null && log.e_a !=null && log.e_c.equals("userBehavior") && log.e_a.equals("lessonPlaybackTriggered")
    }).map(x => {
      VideoPlaybackAgg(
          1,
          x.date
        )
      }).groupBy("date").agg(
      sum("cnt").alias("cnt")
    )
    records.show()
    PersistenceHelper.save(localDevEnv, records.toDF(), config.getEnvironmentString("result.client.videoPlaybacks"), "date", processFromStart)
    println("Execution duration " + ((System.nanoTime() - startTime) / 1000000000.0))
    spark.stop()
  }
}
final case class VideoPlaybackRaw(
                                   e_a: String,
                                   e_c: String,
                                   e_n: String,
                                   date:Date
                                 )
final case class VideoPlaybackAgg(
                                   cnt:Long,
                                   date:Date
                                 )