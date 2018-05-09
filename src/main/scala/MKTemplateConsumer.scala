
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MKTemplateConsumer extends Logging{

  def main(args: Array[String]) {
    val config = new ConfigHelper(this)
    val localDevEnv = config.getBoolean("localDev")
    val processFromStart = config.getBoolean("processFromStart")


    // Template: Specify permanent storage Parquet file
    val permanentStorage = config.getString("permanentStorage")


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> (if (localDevEnv) config.getString("kafka.server_localDev") else config.getString("kafka.server")),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer-group",
      "auto.offset.reset" -> (if(processFromStart) "earliest" else "latest"),
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    // Template: Specify Kafka topic to stream from
    val configuredTopic = String.format(config.getString("kafka.topic"),config.getString("environment"))

    val topics = Array(configuredTopic)
    val sparkConf = new SparkConf()
      .setAppName("MKKafkaConsumer")
    if(localDevEnv) sparkConf.setMaster("local")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("/tmp/log-analyzer-streaming")
    ssc.sparkContext.setLogLevel("ERROR")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val schema = StructType(
        StructField("templateField", LongType, nullable = true)::
          Nil
    )

    val sparkSession = SparkSessionSingleton.getInstance(ssc.sparkContext.getConf)
    if(processFromStart) {
      PersistenceHelper.saveToParquetStorage(
        sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema),
        permanentStorage,
        partitionBy = null,
        overwrite = true
      )
    }

    val messages = stream.map(_.value)

    messages.print()

    logInfo("start the computation...")
    ssc.start()
    ssc.awaitTermination()
    logInfo("computation done!")

  }

  }
