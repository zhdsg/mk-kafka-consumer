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
      .setAppName("MKKafkaConsumer")
      .set("spark.cores.max", "1")
      .set("spark.streaming.concurrentJobs", "2")

    if (localDevEnv) {
      sparkConf.setMaster("local")
    } else {
      sparkConf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("/tmp/log-analyzer-streaming")
    if (localDevEnv) {
      ssc.sparkContext.setLogLevel("ERROR")
    } else {
      ssc.sparkContext.setLogLevel("INFO")
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
      PersistenceHelper.delete(localDevEnv,storageClient)
      PersistenceHelper.delete(localDevEnv,storageServerPayment)
      PersistenceHelper.delete(localDevEnv,storageServerRefund)
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

          df.show()
          PersistenceHelper.save(localDevEnv, toSave, storageClient, "date")
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

          payments.show()
          refunds.show()

          PersistenceHelper.save(localDevEnv, payments, storageServerPayment)
          PersistenceHelper.save(localDevEnv, refunds, storageServerRefund)
        }
      })

    logInfo("Start the computation...")
    ssc.start()
    ssc.awaitTermination()
    logInfo("Computation done!")

  }

}