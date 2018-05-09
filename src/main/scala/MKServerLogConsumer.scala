/**
  * Created by yaning on 5/2/18.
  */

import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object MKServerLogConsumer extends Logging {
  def main(args: Array[String]) {
    val config = new ConfigHelper(this)
    val localDevEnv = config.getBoolean("localDev")
    val processFromStart = config.getBoolean("processFromStart")
    val permanentStorage = config.getString("permanentStorage")

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
    if (localDevEnv) sparkConf.setMaster("local")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    //Because of updateStateByKey requires this
    ssc.checkpoint("/tmp/yaning-log-analyzer-streaming")
    ssc.sparkContext.setLogLevel("ERROR")

    // Create direct kafka stream with brokers and topics
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val sparkSession = SparkSessionSingleton.getInstance(ssc.sparkContext.getConf)

    val schema = StructType(
      StructField("userId", StringType, true) ::
      StructField("payTime", DateType, true) ::
        StructField("payChannel", StringType, true) ::
        StructField("totalPrice", LongType, true) :: Nil
    )


    if (processFromStart) {
      PersistenceHelper.saveToParquetStorage(
        sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema),
        permanentStorage,
        null,
        true
      )
    }

    //Filter out kafka metadata
    val messages = stream.map(_.value)
    messages.foreachRDD { rdd=>
      if(!rdd.partitions.isEmpty){
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        import spark.implicits._
        val structuredData = spark.read.option("multiline", true).option("mode", "PERMISSIVE").json(rdd.toDS()).as[Payment]
        val columns = Seq("userId", "payTime", "payChannel", "totalPrice")
        val df = structuredData.toDF(columns: _*) // create a dataframe from the schema RDD
        df.show(1000)
        logInfo("about to write to parquet!")
        PersistenceHelper.saveToParquetStorage(df, permanentStorage)
        spark.sql("SELECT * FROM parquet.`" + permanentStorage + "` GROUP BY date").show(1000)

      }
      else{
        logInfo("got empty rdd!")
      }
    }

    logInfo("start the computation...")
    ssc.start()
    ssc.awaitTermination()
    logInfo("computation done!")
  }


}
case class Payment(studentId:String,payTime:DateType,payChannel:String,totalPrice:LongType)