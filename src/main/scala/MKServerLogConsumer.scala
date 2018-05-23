/**
  * Created by yaning on 5/2/18.
  */

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import java.sql.{Date, Timestamp}

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.internal.Logging
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{from_unixtime, to_date}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent



object MKServerLogConsumer extends Logging {
  def main(args: Array[String]) {
    val config = new ConfigHelper(this)
    val localDevEnv = config.getBoolean("localDev")
    val processFromStart = config.getBoolean("processFromStart")
    val permanentStoragePayment = config.getString("hiveStorage_payment")
    val permanentStorageRefund = config.getString("hiveStorage_refund")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> (if (localDevEnv) config.getString("kafka.server_localDev") else config.getString("kafka.server")),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer-group",
      "auto.offset.reset" -> (if (processFromStart) "earliest" else "latest"),
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val configedTopic = String.format(config.getString("kafka.topic"),config.getString("environment"))
    val topics = Array(configedTopic)
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf()
      .setAppName("MKKafkaConsumer")
      .set("spark.sql.warehouse.dir","/user/hive/warehouse")
    if (localDevEnv) sparkConf.setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    //Because of updateStateByKey requires this
    ssc.checkpoint("/tmp/root-log-analyzer-streaming")
    ssc.sparkContext.setLogLevel("ERROR")

    // Create direct kafka stream with brokers and topics
    val stream =
      KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
//    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val sparkSession = SparkSessionSingleton.getInstance(ssc.sparkContext.getConf)


    val schemaPay = StructType(
      StructField("actionType", StringType, true) ::
      StructField("userId", LongType, true) ::
        StructField("purchaseNumber", StringType, true) ::
        StructField("payTime", DateType, true) ::
        StructField("payChannel", LongType, true) ::
        StructField("totalPrice", LongType, true) ::
        StructField("date", DateType, true) :: Nil
    )

    val schemaRefund = StructType(
      StructField("actionType", StringType, true) ::
        StructField("userId", LongType, true) ::
        StructField("verifyTime", DateType, true) ::
        StructField("classId", LongType, true) ::
        StructField("purchaseNumber", StringType, true) ::
        StructField("purchaseMoney", LongType, true) ::
        StructField("refundMoney", LongType, true) ::
        StructField("status", LongType, true) ::
        StructField("statusName", StringType, true) ::
        StructField("date", DateType, true) :: Nil
    )


    if (processFromStart) {
      PersistenceHelper.saveToHive(
        sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schemaPay),
        permanentStoragePayment,
        "date",
        true
      )
      PersistenceHelper.saveToHive(
        sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schemaRefund),
        permanentStorageRefund,
        "date",
        true
      )
    }
//    var offsetRanges = Array[OffsetRange]()
//    val lines= stream.transform{rdd=>
//      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      rdd
//    }.map(_.value())
//    lines.print()
    //Filter out kafka metadata
    val messages = stream.filter(x=>{
      x.value().length>ConsUtil.MK_SERVER_LOG_ROW_OFFSET
    }).map(x => {
      x.value().substring(ConsUtil.MK_SERVER_LOG_ROW_OFFSET)//get rid of time string in the beginning of each row
    })
//    val messages= lines
    val structuredMessages = messages
      .transform(rdd=> {
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        //Transform String rdd into structured DataFrame by parsing JSON
        import spark.implicits._
        val ds = spark.createDataset[String](rdd)
        val df = spark.read.json(ds)

        if(rdd.isEmpty() || !df.columns.contains("actionType")){
          df.rdd
        }else{
          df.filter(x => {
            (x.getAs[String]("actionType")!=null )
          }).rdd
        }
      })
    .filter(row => row.length>0)

    val payments = structuredMessages.filter(x=>{
      (x.getAs[String]("actionType").equals(ConsUtil.PAY_ACTION))
    }).map(x=>(x.getAs[String]("purchaseNumber"),
        (x.getAs[String]("actionType"), x.getAs[Long]("studentId"),
          x.getAs[String]("purchaseNumber"),
          new Date(x.getAs[Long]("payTime")),
          x.getAs[Long]("payChannel"),
          x.getAs[Long]("totalPrice"),
          new Date(x.getAs[Long]("payTime")))
      )).reduceByKey{case(a,b)=>a}.map(_._2)

    payments.foreachRDD(rdd=>{
      if(!rdd.isEmpty()){

        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        val columns = Seq("actionType", "userId", "purchaseNumber", "payTime", "payChannel","totalPrice","date")
        val df = spark.createDataFrame(rdd).toDF(columns: _*)
        logError("about to show df!")
        df.show()
        logError("about to write pay log to parquet!")
        PersistenceHelper.saveToHive(df, permanentStoragePayment,"date")
        logError("about to show parquet!")
        spark.sql("SELECT actionType, userId, purchaseNumber, payTime, payChannel, totalPrice, date FROM " + permanentStoragePayment).show()
//        payments.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })

    val refunds =
      structuredMessages.filter(x=>{
        (x.getAs[String]("actionType").equals(ConsUtil.REFUND_SUCCESS_ACTION)
          ||x.getAs[String]("actionType").equals(ConsUtil.REFUND_VERIFICATION_FAILED_ACTION)
          ||x.getAs[String]("actionType").equals(ConsUtil.REFUND_APPLY_ACTION)
          ||x.getAs[String]("actionType").equals(ConsUtil.REFUND_CANCELLED_ACTION)
          ||x.getAs[String]("actionType").equals(ConsUtil.REFUND_VERIFICATION_FAILED_ACTION))
      }).map(x=>(x.getAs[String]("purchaseNumber"),
        (x.getAs[String]("actionType"), x.getAs[Long]("studentId"),
          new Date(x.getAs[Long]("verifyTime")), x.getAs[Long]("classId"),
          x.getAs[String]("purchaseNumber"),x.getAs[Long]("purchaseMoney"), x.getAs[Long]("money"),
          x.getAs[Long]("status"),x.getAs[Long]("status") match {
            case ConsUtil.toBeVerify => ConsUtil.toBeVerifyStr
            case ConsUtil.verifyFailed => ConsUtil.verifyFailedStr
            case ConsUtil.cashRefunded => ConsUtil.cashRefundedStr
            case ConsUtil.canceled => ConsUtil.canceledStr
            case ConsUtil.refundInProgress => ConsUtil.refundInProgressStr
            case ConsUtil.onlineRefunded => ConsUtil.onlineRefundedStr
            case ConsUtil.workingInProgress => ConsUtil.workingInProgressStr
          } ,
          new Date(x.getAs[Long]("verifyTime")))
        )).reduceByKey{case(a,b)=>a}.map(_._2)

    refunds.foreachRDD(rdd=>{
      if(!rdd.isEmpty()){
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        val columns = Seq("actionType", "userId", "verifyTime", "classId","purchaseNumber","purchaseMoney","refundMoney","status","statusName","date")
        val df = spark.createDataFrame(rdd).toDF(columns: _*)
        logError("about to show df!")
        df.show()
        logError("about to write refund log to parquet!")
        PersistenceHelper.saveToHive(df, permanentStorageRefund,"date")
        logError("about to show parquet!")
        spark.sql("SELECT actionType, userId, verifyTime, classId, purchaseNumber, purchaseMoney, refundMoney,status,statusName, date FROM " + permanentStorageRefund).show()
//        refunds.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })

    logInfo("start the computation...")
    ssc.start()
    ssc.awaitTermination()
    logInfo("computation done!")
  }


}