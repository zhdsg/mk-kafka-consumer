package com.zhimo.datahub.etl.stage2

import java.sql.Date

import com.zhimo.datahub.common.{ConfigHelper, ConsUtil, PersistenceHelper, SparkSessionSingleton}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{last, sum}

/**
  * Created by yaning on 6/13/18.
  */
object MKStage2ServerRevenue  extends Logging{
  def main(args: Array[String]) {
    val config = new ConfigHelper(this)
    val processFromStart = config.getBoolean("processFromStart")
    val localDevEnv = config.getBoolean("localDev")

    // Template: Specify permanent storage Parquet file
    val storage = config.getEnvironmentString("storage.server.payment")


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
    val records = PersistenceHelper.load(localDevEnv, spark, storage).as[PaymentRaw]
      .map(x => {
        PaymentAgg(
          x.purchaseNumber,
          x.totalPrice / 100,
          x.payPrice / 100,
          x.payChannel match {
            case ConsUtil.ALIPAY_IMMEDIATE => ConsUtil.ALIPAY_IMMEDIATE_STR
            case ConsUtil.ALIPAY_QRCODE => ConsUtil.ALIPAY_QRCODE_STR
            case ConsUtil.WEIXIN_JS => ConsUtil.WEIXIN_JS_STR
            case ConsUtil.WXM_PAY => ConsUtil.WXM_PAY_STR
            case ConsUtil.UNION_POS => ConsUtil.UNION_POS_STR
            case ConsUtil.BILL_POS => ConsUtil.BILL_POS_STR
            case ConsUtil.POS => ConsUtil.POS_STR
            case ConsUtil.OFFLINE => ConsUtil.OFFLINE_STR
          },
          if (x.payTime != null) new Date(x.payTime) else Date.valueOf(x.date)
        )
      }).groupBy("purchaseNumber").agg(
      last("payChannel", ignoreNulls = true).alias("payChannel"),
      last("payPrice", ignoreNulls = true).alias("payPrice"),
      last("totalPrice", ignoreNulls = true).alias("totalPrice"),
      last("date", ignoreNulls = true).alias("date")
    ).groupBy("date", "payChannel").agg(
      last("purchaseNumber", ignoreNulls = true).alias("purchaseNumber"),
      sum("payPrice").alias("payPrice"),
      sum("totalPrice").alias("totalPrice")
    )
    records.show()
    PersistenceHelper.save(localDevEnv, records.toDF(), config.getEnvironmentString("result.server.revenue"), "date", processFromStart)
    spark.stop()
  }
}

final case class PaymentRaw(
                        purchaseNumber:String,
                        totalPrice:Long,
                        payPrice:Long,
                        payChannel:Long,
                        payTime:Long,
                        date:String
                        )
final case class PaymentAgg(
                             purchaseNumber:String,
                             totalPrice:Long,
                             payPrice:Long,
                             payChannel:String,
                             date:Date
                           )