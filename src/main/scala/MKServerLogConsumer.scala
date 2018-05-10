/**
  * Created by yaning on 5/2/18.
  */

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.KafkaUtils
import java.sql.{Date, Timestamp}

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.internal.Logging
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


object MKServerLogConsumer extends Logging {
  def main(args: Array[String]) {
    val config = new ConfigHelper(this)
    val localDevEnv = config.getBoolean("localDev")
    val processFromStart = config.getBoolean("processFromStart")
    val permanentStoragePayment = config.getString("permanentStorage_payment")
    val permanentStorageRefund = config.getString("permanentStorage_refund")

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
    if (localDevEnv) sparkConf.setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    //Because of updateStateByKey requires this
    ssc.checkpoint("/tmp/yaning-log-analyzer-streaming")
    ssc.sparkContext.setLogLevel(if(localDevEnv)"ERROR" else "INFO")

    // Create direct kafka stream with brokers and topics
    val stream =
      KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
//    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val sparkSession = SparkSessionSingleton.getInstance(ssc.sparkContext.getConf)


//    val schema = StructType(
//      StructField("actionType", StringType, true) ::
//      StructField("userId", StringType, true) ::
//      StructField("payTime", DateType, true) ::
//        StructField("payChannel", StringType, true) ::
//        StructField("totalPrice", LongType, true) ::
//        StructField("date", DateType, true) :: Nil
//    )
//
//
//    if (processFromStart) {
//      PersistenceHelper.saveToParquetStorage(
//        sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema),
//        permanentStorage,
//        true
//      )
//    }


    //Filter out kafka metadata
    val messages = stream.map(_.value)
//    val messages= lines
    val structuredMessages = messages
      .transform(rdd=> {
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        //Transform String rdd into structured DataFrame by parsing JSON
        import spark.implicits._
        val ds = spark.createDataset[String](rdd)
        val df = spark.read.json(ds)

        if(rdd.isEmpty() || !df.columns.contains("actionType")){
          logInfo("empty rdd!")
          df.rdd
        }else{
          logInfo("parsing rdd:"+df.show())
          df.filter(x => {
            (x.getAs[String]("actionType")!=null )
          }).rdd
        }
      })
    .filter(row => row.length>0)

    val payments = structuredMessages.filter(x=>{
      (x.getAs[String]("actionType").equals(ConsUtil.PAY_ACTION))
    }).map(x=>(
        (x.getAs[String]("actionType"), x.getAs[String]("studentId"),
          Timestamp.valueOf(x.getAs[String]("payTime")), x.getAs[String]("payChannel"),
          x.getAs[Long]("totalPrice"),Timestamp.valueOf(x.getAs[String]("payTime")))
      ))

    payments.foreachRDD(rdd=>{
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      val columns = Seq("actionType", "userId", "payTime", "payChannel","totalPrice","date")
      val df = spark.createDataFrame(rdd).toDF(columns: _*)
      df.show(1000)
      if(!rdd.isEmpty()){
        logInfo("about to write pay log to parquet!")
        PersistenceHelper.saveToParquetStorage(df, permanentStoragePayment)
        spark.sql("SELECT actionType, userId, payTime, payChannel, totalPrice, date FROM parquet.`" + permanentStoragePayment + "`").show(1000)
      }
    })

    val refunds =
      structuredMessages.filter(x=>{
        (x.getAs[String]("actionType").equals(ConsUtil.REFUND_SUCCESS_ACTION)
          ||x.getAs[String]("actionType").equals(ConsUtil.REFUND_VERIFICATION_FAILED_ACTION)
          ||x.getAs[String]("actionType").equals(ConsUtil.REFUND_APPLY_ACTION)
          ||x.getAs[String]("actionType").equals(ConsUtil.REFUND_CANCELLED_ACTION)
          ||x.getAs[String]("actionType").equals(ConsUtil.REFUND_VERIFICATION_FAILED_ACTION))
      }).map(x=>(
        (x.getAs[String]("actionType"), x.getAs[String]("studentId"),
          Timestamp.valueOf(x.getAs[String]("verifyTime")), x.getAs[String]("classId"),
          x.getAs[String]("purchaseId"),x.getAs[Long]("purchaseMoney"), x.getAs[Long]("money"),
          x.getAs[Long]("status"),x.getAs[Long]("status") match {
            case ConsUtil.toBeVerify => ConsUtil.toBeVerifyStr
            case ConsUtil.verifyFailed => ConsUtil.verifyFailedStr
            case ConsUtil.cashRefunded => ConsUtil.cashRefundedStr
            case ConsUtil.canceled => ConsUtil.canceledStr
            case ConsUtil.refundInProgress => ConsUtil.refundInProgressStr
            case ConsUtil.onlineRefunded => ConsUtil.onlineRefundedStr
            case ConsUtil.workingInProgress => ConsUtil.workingInProgressStr
          } ,
          Timestamp.valueOf(x.getAs[String]("verifyTime")))
        ))

    refunds.foreachRDD(rdd=>{
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      val columns = Seq("actionType", "userId", "verifyTime", "classId","purchaseId","purchaseMoney","refundMoney","stats","statsName","date")
      val df = spark.createDataFrame(rdd).toDF(columns: _*)
      df.show(1000)
      if(!rdd.isEmpty()){
        logInfo("about to write refund log to parquet!")
        PersistenceHelper.saveToParquetStorage(df, permanentStorageRefund)
        spark.sql("SELECT actionType, userId, verifyTime, classId, purchaseId, purchaseMoney, refundMoney,stats,statsName, date FROM parquet.`" + permanentStorageRefund + "`").show(1000)
      }
    })

    logInfo("start the computation...")
    ssc.start()
    ssc.awaitTermination()
    logInfo("computation done!")
  }


}