package com.zhimo.datahub.etl.stage1


import com.zhimo.datahub.common.{ConfigHelper, ConsUtil, PersistenceHelper, SparkSessionSingleton}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{from_unixtime, to_date}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
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
    val configuredTopicClient = config.getEnvironmentString("kafka.topic.client")
    val configuredTopicServer = config.getEnvironmentString("kafka.topic.server")


    val sparkConf = new SparkConf()
      .setAppName(ConfigHelper.getClassName(this))
      .set("spark.cores.max", "1")
      .set("spark.streaming.concurrentJobs", "2")

    if (localDevEnv) {
      sparkConf.setMaster("local")
    } else {
      sparkConf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(config.getInt("kafka.interval")))
    ssc.checkpoint("/tmp/log-analyzer-streaming")
    if (localDevEnv) {
      ssc.sparkContext.setLogLevel("ERROR")
    } else {
      ssc.sparkContext.setLogLevel("ERROR")
    }

    val streamClient = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Array(configuredTopicClient), kafkaParams)
    )


    val streamServer = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Array(configuredTopicServer), kafkaParams)
    )

    val sparkSession = SparkSessionSingleton.getInstance(ssc.sparkContext.getConf, !localDevEnv)

    if (processFromStart) { // Clean up storage if processing from start
      PersistenceHelper.deleteParquet(storageClient)
      PersistenceHelper.deleteParquet(storageServerPayment)
      PersistenceHelper.deleteParquet(storageServerRefund)
      PersistenceHelper.deleteParquet(storageServerStudent)
      PersistenceHelper.deleteParquet(storageServerSignup)
    }

    streamClient
      .map(_.value)
      .foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf, !localDevEnv)
          import spark.implicits._
          val ds = spark.createDataset[String](rdd)
          val df = spark.read.json(ds)

          val toSave = df
            .filter(x => {
              x.getAs[Any]("t") != null
            })
            .withColumn("date", to_date(from_unixtime(df("t") / 1000)))

          PersistenceHelper.saveToParquetStorage(toSave, storageClient)
          df.show()
        }
      })

    streamServer
      .map(_.value)
      .filter(_.length > ConsUtil.MK_SERVER_LOG_ROW_OFFSET)
      .map(x=>(
        "{\"date\":\"".concat(x.substring(0,ConsUtil.MK_SERVER_LOG_DATE_OFFSET)).concat("\",").concat(x.substring(ConsUtil.MK_SERVER_LOG_ROW_OFFSET+1))
        ))
      .foreachRDD(rdd => {
        if(!rdd.isEmpty()) {
          val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
          import spark.implicits._
          val ds = spark.createDataset[String](rdd)
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
          payments.printSchema()
          payments.show()
          refunds.printSchema()
          refunds.show()
          students.printSchema()
          students.show()
          signups.printSchema()
          signups.show()

          PersistenceHelper.saveToParquetStorage(payments, storageServerPayment, "date")
          PersistenceHelper.saveToParquetStorage(refunds, storageServerRefund, "date")
          PersistenceHelper.saveToParquetStorage(students, storageServerStudent, "date")
          PersistenceHelper.saveToParquetStorage(signups, storageServerSignup, "date")

        }
      })

    logInfo("Start the computation...")
    ssc.start()
    ssc.awaitTermination()
    logInfo("Computation done!")

  }

}
