package com.zhimo.datahub.etl.stage1


import com.zhimo.datahub.common._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{from_unixtime, to_date}
import org.apache.spark.sql.types.{StringType, StructType, StructField}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MKStage1All extends Logging {

  def main(args: Array[String]) {
    val config = new ConfigHelper(this)
    val localDevEnv = config.getBoolean("localDev")
    val processFromStart = config.getBoolean("processFromStart")


    val storageClient = config.getEnvironmentString("storage.client")
    val storageServerPayment = config.getEnvironmentString("storage.server.payment")
    val storageServerRefund = config.getEnvironmentString("storage.server.refund")
    val storageServerStudent = config.getEnvironmentString("storage.server.student")
    val storageServerSignup = config.getEnvironmentString("storage.server.signup")


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> (if (localDevEnv) config.getString("kafka.server_localDev") else config.getString("kafka.server")),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer-group",
      "auto.offset.reset" -> (if (processFromStart) "earliest" else "latest"),
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    // Template: Specify Kafka topic to stream from
    val configuredTopicClient = config.getEnvironmentString("kafka.topic.client").split(",")
    val configuredTopicServer = config.getEnvironmentString("kafka.topic.server").split(",")


    val sparkConf = new SparkConf()
      .setAppName(ConfigHelper.getClassName(this))
      .set("spark.cores.max", "1")
      .set("spark.streaming.concurrentJobs", "2")

    if (localDevEnv) {
      sparkConf.setMaster("local")
    } else {
      sparkConf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(
      if(localDevEnv) 2 else config.getInt("kafka.interval")
    ))

    ssc.checkpoint("/tmp/log-analyzer-streaming")
       if (localDevEnv) {
      ssc.sparkContext.setLogLevel("ERROR")
    } else {
      ssc.sparkContext.setLogLevel("ERROR")
    }
    val offsetManager = new OffsetManager()
   // val bc = ssc.sparkContext.broadcast[ConfigHelper](config)
    //冷启动 offset 为空
    val streamClient ={
      offsetManager.readOffset(configuredTopicClient,config) match{
        case Some(offsets) =>
          KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](configuredTopicClient,kafkaParams,offsets)
          )
        case None =>
          if (processFromStart) { // Clean up storage if processing from start
            PersistenceHelper.deleteParquet(storageClient)
          }
          KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](configuredTopicClient, kafkaParams)
          )
      }

    }
    val streamServer ={
      offsetManager.readOffset(configuredTopicServer,config ) match{
        case Some(offsets) =>
          KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](configuredTopicServer, kafkaParams,offsets)
          )
        case None =>
          if (processFromStart) { // Clean up storage if processing from start
            PersistenceHelper.deleteParquet(storageServerPayment)
            PersistenceHelper.deleteParquet(storageServerRefund)
            PersistenceHelper.deleteParquet(storageServerStudent)
            PersistenceHelper.deleteParquet(storageServerSignup)
          }
          KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](configuredTopicServer, kafkaParams)
          )
      }

    }
    val sparkSession = SparkSessionSingleton.getInstance(ssc.sparkContext.getConf, !localDevEnv)

//    if (processFromStart) { // Clean up storage if processing from start
//      PersistenceHelper.deleteParquet(storageClient)
//      PersistenceHelper.deleteParquet(storageServerPayment)
//      PersistenceHelper.deleteParquet(storageServerRefund)
//      PersistenceHelper.deleteParquet(storageServerStudent)
//      PersistenceHelper.deleteParquet(storageServerSignup)
//    }

    streamClient

      .foreachRDD(rdd => {
        //get kafka offset
        val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetManager.writeOffset(offsetRange,new ConfigHelper(this),true)
        if (!rdd.isEmpty()) {
          val values =rdd.map(_.value)
          val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf, !localDevEnv)
          import spark.implicits._
          val ds = spark.createDataset[String](values)
          val df= spark.read.json(ds)

          val toSave = df.toDF()
            .filter(x => {
              x.getAs[Any]("t") != null
            })
            .withColumn("date", to_date(from_unixtime(df("t") / 1000)))

          PersistenceHelper.saveToParquetStorage(toSave, storageClient)
         // df.show()

        }
        offsetManager.writeOffset(offsetRange,new ConfigHelper(this),false)
      })

    streamServer
      .foreachRDD(rdd => {
        //get kafka offset
        val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetManager.writeOffset(offsetRange,new ConfigHelper(this),true)
        val values =rdd.map(_.value)
          .filter(_.length > ConsUtil.MK_SERVER_LOG_ROW_OFFSET)
          .map(x=>(
            "{\"date\":\"".concat(x.substring(0,ConsUtil.MK_SERVER_LOG_DATE_OFFSET)).concat("\",").concat(x.substring(ConsUtil.MK_SERVER_LOG_ROW_OFFSET+1))
            ))

        if(!values.isEmpty()) {

          val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
          import spark.implicits._
          val ds = spark.createDataset[String](values)
          val df = spark.read.json(ds)

          val payments = df.filter(x => {
            x.getAs[String]("actionType").equals(ConsUtil.PAY_ACTION)
          })

          val refunds = df.filter(x => {
            val action = x.getAs[String]("actionType")
            val refundActions = Array(
              ConsUtil.REFUND_SUCCESS_ACTION,
              ConsUtil.REFUND_VERIFICATION_FAILED_ACTION,
              ConsUtil.REFUND_APPLY_ACTION,
              ConsUtil.REFUND_CANCELLED_ACTION,
              ConsUtil.REFUND_VERIFICATION_FAILED_ACTION
            )
            refundActions.contains(action)
          })

          val students = df.filter(x => {
            x.getAs[String]("actionType").equals(ConsUtil.ADD_STUDENT)
          })
          val signups = df.filter(x => {
            x.getAs[String]("actionType").equals(ConsUtil.SIGNUP_CLASS)
          })
//          payments.printSchema()
//          payments.show()
//          refunds.printSchema()
//          refunds.show()
//          students.printSchema()
//          students.show()
//          signups.printSchema()
//          signups.show()

          PersistenceHelper.saveToParquetStorage(payments, storageServerPayment, "date")
          PersistenceHelper.saveToParquetStorage(refunds, storageServerRefund, "date")
          PersistenceHelper.saveToParquetStorage(students, storageServerStudent, "date")
          PersistenceHelper.saveToParquetStorage(signups, storageServerSignup, "date")

        }

        offsetManager.writeOffset(offsetRange,new ConfigHelper(this),false)
      })

    logInfo("Start the computation...")
    ssc.start()
    ssc.awaitTermination()
    logInfo("Computation done!")

  }

}
