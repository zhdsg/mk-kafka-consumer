package com.zhimo.datahub.etl.stage2

import com.zhimo.datahub.common.{ConfigHelper, Geo2IPHelper, SparkSessionSingleton}
import org.apache.spark.SparkConf

object MKStage2PrepareGeolocation {

  def main(args: Array[String]) {
    val startTime = System.nanoTime()
    val config = new ConfigHelper(this)
    val processFromStart = config.getBoolean("processFromStart")
    val localDevEnv = config.getBoolean("localDev")
    val showResults = config.getBoolean("showResults")

    // Template: Specify permanent storage Parquet file
    val storage = config.getEnvironmentString("storage.client")


    val sparkConf = new SparkConf()
      .setAppName("MKKafkaConsumer")
    if (localDevEnv) {
      sparkConf.setMaster("local")
    } else {
      sparkConf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")
    }

    val spark = SparkSessionSingleton.getInstance(sparkConf, !localDevEnv)

    if (localDevEnv) {
      spark.sparkContext.setLogLevel("ERROR")
    }

    println("Before analysis " + ((System.nanoTime() - startTime) / 1000000000.0))

    Geo2IPHelper.init(localDevEnv, spark,forceOverwrite = true)

    spark.stop()
    println("Execution duration " + ((System.nanoTime() - startTime) / 1000000000.0))
  }
}
