package com.zhimo.datahub.etl

import java.sql.Date

import com.zhimo.datahub.common.{ConfigHelper, ConsUtil, PersistenceHelper, SparkSessionSingleton}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{last, sum,count}

/**
  * Created by yaning on 6/20/18.
  */
object MKStage2ServerRefund extends Logging{
  def main(args: Array[String]) {
    val startTime = System.nanoTime()
    val config = new ConfigHelper(this)
    val processFromStart = config.getBoolean("processFromStart")
    val localDevEnv = config.getBoolean("localDev")

    // Template: Specify permanent storage Parquet file
    val storage = config.getEnvironmentString("storage.server.refund")


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
    val records = PersistenceHelper.load(localDevEnv, spark, storage).as[RefundRaw]
      .map(x => {
        RefundAgg(
          x.purchaseId,
          x.money / 100,
          x.purchaseMoney / 100,
          x.status match {
            case ConsUtil.toBeVerify => ConsUtil.toBeVerifyStr
            case ConsUtil.verifyFailed => ConsUtil.verifyFailedStr
            case ConsUtil.cashRefunded => ConsUtil.cashRefundedStr
            case ConsUtil.canceled => ConsUtil.canceledStr
            case ConsUtil.refundInProgress => ConsUtil.refundInProgressStr
            case ConsUtil.onlineRefunded => ConsUtil.onlineRefundedStr
            case ConsUtil.workingInProgress => ConsUtil.workingInProgressStr
            case others => ConsUtil.defaultStatsStr
          },
          if (x.verifyTime != null) new Date(x.verifyTime)
          else if (x.updateTime != null) new Date(x.updateTime)
          else Date.valueOf(x.date)
        )
      }).groupBy("purchaseId").agg(
      last("refundMoney", ignoreNulls = true).alias("refundMoney"),
      last("purchaseMoney", ignoreNulls = true).alias("purchaseMoney"),
      last("status", ignoreNulls = true).alias("status"),
      last("date", ignoreNulls = true).alias("date")
    ).groupBy("date", "status").agg(
      count("purchaseId").alias("cnt"),
      sum("refundMoney").alias("refundMoney"),
      sum("purchaseMoney").alias("purchaseMoney")
    )
    records.show()
    PersistenceHelper.save(localDevEnv, records.toDF(), config.getEnvironmentString("result.server.refund"), "date", processFromStart)
    println("Execution duration " + ((System.nanoTime() - startTime) / 1000000000.0))
    spark.stop()
  }
}

final case class RefundRaw(
                             purchaseId:Long,
                             purchaseNumber:String,
                             money:Long,
                             purchaseMoney:Long,
                             status:Long,
                             updateTime:Long,
                             verifyTime:Long,
//                             classId:Long,
//                             courseId:Long,
                             date:String
                           )
final case class RefundAgg(
                             purchaseId:Long,
                             refundMoney:Long,
                             purchaseMoney:Long,
                             status:String,
                             date:Date
                           )
