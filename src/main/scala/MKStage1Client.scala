
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{from_unixtime, to_date}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MKStage1Client extends Logging {

  def main(args: Array[String]) {
    val config = new ConfigHelper(this)
    val localDevEnv = config.getBoolean("localDev")
    val processFromStart = config.getBoolean("processFromStart")


    // Template: Specify permanent storage Parquet file
    val permanentStorage = config.getEnvironmentString("permanentStorage")


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> (if (localDevEnv) config.getString("kafka.server_localDev") else config.getString("kafka.server")),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer-group",
      "auto.offset.reset" -> (if (processFromStart) "earliest" else "latest"),
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    // Template: Specify Kafka topic to stream from
    val configuredTopic = String.format(config.getString("kafka.topic"), config.getString("environment"))

    val topics = Array(configuredTopic)
    val sparkConf = new SparkConf()
      .setAppName("MKKafkaConsumer")
    if(localDevEnv){
      sparkConf.setMaster("local")
    }else{
      sparkConf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("/tmp/log-analyzer-streaming")
    ssc.sparkContext.setLogLevel("ERROR")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val schema = StructType(
      Nil
    )

    val sparkSession = SparkSessionSingleton.getInstance(ssc.sparkContext.getConf, !localDevEnv)
    if (processFromStart) {
      PersistenceHelper.saveToParquetStorage(
        sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema),
        permanentStorage,
        partitionBy = null,
        overwrite = true
      )
    }

    val messages = stream.map(_.value)

    messages
      .foreachRDD(rdd => {
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf, !localDevEnv)
        //Transform String rdd into structured DataFrame by parsing JSON
        import spark.implicits._
        val ds = spark.createDataset[String](rdd)
        val df = spark.read.json(ds)


        if (rdd.isEmpty() || !df.columns.contains("t")) {
          df.rdd
        } else {
          val toSave = df
            .filter(x => {
              x.getAs[Any]("t") != null
            })
            //Add new column to DataFrame: DateType parsed from timestamp
            .withColumn("date", to_date(from_unixtime(df("t") / 1000)))

          if(localDevEnv) {
            PersistenceHelper.saveToParquetStorage(toSave, permanentStorage, "date")
            spark.sql("SELECT * FROM parquet.`" + permanentStorage + "`").show()
          }else{
            PersistenceHelper.saveToHive(toSave, permanentStorage, "date")
          }
        }
      })



    logInfo("start the computation...")
    ssc.start()
    ssc.awaitTermination()
    logInfo("computation done!")

  }

}
