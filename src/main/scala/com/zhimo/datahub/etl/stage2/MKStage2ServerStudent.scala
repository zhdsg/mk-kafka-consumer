package com.zhimo.datahub.etl.stage2

import java.sql.Date

import com.zhimo.datahub.common.{ConfigHelper, PersistenceHelper, SparkSessionSingleton}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{last, count}

/**
  * Created by yaning on 6/22/18.
  */
object MKStage2ServerStudent extends Logging{
  def main(args: Array[String]) {
    val startTime = System.nanoTime()
    val config = new ConfigHelper(this)
    val processFromStart = config.getBoolean("processFromStart")
    val localDevEnv = config.getBoolean("localDev")

    // Template: Specify permanent storage Parquet file
    val storage = config.getEnvironmentString("storage.server.student")


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
    val records = PersistenceHelper.load(localDevEnv, spark, storage).as[StudentRaw]
      .map(x => {
        StudentAgg(
          x.studentId,
          x.sex match {
            case 0 => "男"
            case 1 => "女"
            case -1 => "未知"
          },
          Date.valueOf(x.date)
        )
      }).groupBy("studentId").agg(
      last("sex", ignoreNulls = true).alias("sex"),
      last("date", ignoreNulls = true).alias("date")
    ).groupBy("date", "sex").agg(
      count("studentId").alias("cnt")
    )
    records.show()
    PersistenceHelper.save(localDevEnv, records.toDF(), config.getEnvironmentString("result.server.student"), "date", processFromStart)
    println("Execution duration " + ((System.nanoTime() - startTime) / 1000000000.0))
    spark.stop()
  }
}

final case class StudentRaw(
                             studentId:Long,
                             name:String,
                             sex:Long,
                             mobile:String,
                             date:String
                           )
final case class StudentAgg(
                             studentId:Long,
                             sex:String,
                             date:Date
                           )
