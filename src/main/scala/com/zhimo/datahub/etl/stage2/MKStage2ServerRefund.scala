package com.zhimo.datahub.etl.stage2

import java.sql.Date

import com.zhimo.datahub.common.{ConfigHelper, ConsUtil, PersistenceHelper, SparkSessionSingleton}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{count, sum}
import org.apache.spark.sql.types.LongType

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
      .setAppName("MKStage2ServerRefund")
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
    val records =rawRecords.as[RefundRaw]
      .map(x => {
        RefundAgg(
          x.purchaseId,
          x.money / 100,
          x.purchaseMoney / 100,
          x.actionType
          match {
            case ConsUtil.REFUND_APPLY_ACTION=> ConsUtil.toBeVerifyStr
            case ConsUtil.REFUND_VERIFICATION_FAILED_ACTION => ConsUtil.verifyFailedStr
            case ConsUtil.REFUND_SUCCESS_ACTION => ConsUtil.refundedStr
            case ConsUtil.REFUND_CANCELLED_ACTION => ConsUtil.canceledStr
          }
          ,
//          if (x.verifyTime.getOrElse(null) != null) new Date(x.verifyTime.get)
//          else if (x.updateTime.getOrElse(null) != null) new Date(x.updateTime.get)
//          else Date.valueOf(x.date)
          Date.valueOf(x.date)
        )
      })
      .groupBy("date","status").agg(
      count("purchaseId").alias("cnt"),
      sum("refundMoney").alias("refundMoney"),
      sum("purchaseMoney").alias("purchaseMoney")
    )
    records.printSchema()
    records.show()
    PersistenceHelper.save(localDevEnv, records.toDF(), config.getEnvironmentString("result.server.refund"), "date", processFromStart)
    println("Execution duration " + ((System.nanoTime() - startTime) / 1000000000.0))
    spark.stop()
  }
}

final case class RefundRaw(
                            actionType:String,
                            purchaseId:Long,
                            money:Long,
                            purchaseMoney:Long,
//                            status:Option[Long],
                            //                             updateTime:Option[Long],
                            //                             verifyTime:Option[Long],
                            date:String
                           )
final case class RefundAgg(
                             purchaseId:Long,
                             refundMoney:Long,
                             purchaseMoney:Long,
                             status:String,
                             date:Date
                           )
