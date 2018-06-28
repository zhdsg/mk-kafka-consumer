package com.zhimo.datahub.etl.stage2

import java.sql.Date

import com.zhimo.datahub.common.{ConfigHelper, PersistenceHelper, SparkSessionSingleton}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{count}

/**
  * Created by yaning on 6/22/18.
  */
object MKStage2ServerSignup extends Logging{
  def main(args: Array[String]) {
    val startTime = System.nanoTime()
    val config = new ConfigHelper(this)
    val processFromStart = config.getBoolean("processFromStart")
    val localDevEnv = config.getBoolean("localDev")

    // Template: Specify permanent storage Parquet file
    val storage = config.getEnvironmentString("storage.server.signup")


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

    import spark.implicits._
    //TODO: load raw data
    val rawRecords = PersistenceHelper.loadFromParquet(spark, storage)
    rawRecords.printSchema()
    rawRecords.show()
    val records =rawRecords.as[SignupRaw]
      .flatMap(x => {
        val pid = x.purchaseId
        val date = Date.valueOf(x.date)
        val classNames:List[String] = x.purchaseClassNames
        classNames.map(y=>
          SignupAgg(
            pid,
            y,
            date
          )
        )
      })
      .groupBy("date", "className").agg(
      count("purchaseId").alias("cnt")
    )
    records.show()
    PersistenceHelper.save(localDevEnv, records.toDF(), config.getEnvironmentString("result.server.signup"), "date", processFromStart)
    println("Execution duration " + ((System.nanoTime() - startTime) / 1000000000.0))
    spark.stop()
  }
}

final case class SignupRaw(
                             purchaseId:Long,
                             studentId:Long,
                             studentName:String,
                             purchaseClassIds:List[Long],
                             purchaseClassNames:List[String],
                             date:String
                           )
final case class SignupAgg(
                            purchaseId:Long,
                            className:String,
                            date:Date
                           )
