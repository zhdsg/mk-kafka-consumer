import java.sql.Date

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.last
import org.apache.spark.sql.functions.sum

/**
  * Created by yaning on 6/13/18.
  */
object MKStage2ServerRevenue  extends Logging{
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
  val records = PersistenceHelper.load(localDevEnv, spark, storage).as[Payment]
  .map(x=>(
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
    x.payPrice / 100,
    new Date(x.payTime),
    x.date,
    x.purchaseNumber
  )).groupBy("purchaseNumber").agg(
    last("payChannel", ignoreNulls = true).alias("payChannel"),
    last("payPrice", ignoreNulls = true).alias("payPrice"),
    last("payTime", ignoreNulls = true).alias("date"),
    last("date", ignoreNulls = true).alias("logDate")
  ).groupBy("date","payChannel").agg(
    last("purchaseNumber", ignoreNulls = true).alias("purchaseNumber"),
    last("logDate", ignoreNulls = true).alias("logDate"),
    sum("payPrice").alias("totalPrice")
  )
  records.show()
  PersistenceHelper.save(localDevEnv, records.toDF(), config.getEnvironmentString("finance.revenue"), "date", processFromStart)
  
}

final case class Payment(
                        purchaseNumber:String,
                        totalPrice:Long,
                        payPrice:Long,
                        payChannel:Long,
                        payTime:Long,
                        date:Date
                        )