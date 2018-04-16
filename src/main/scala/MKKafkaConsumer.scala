/**
  * Created by raven on 29/03/2018.
  */

import kafka.log.Log
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.internal.Logging
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_unixtime, to_date}
import org.apache.spark.sql.types.DateType

object MKKafkaConsumer extends Logging {
  def main(args: Array[String]) {
    val config = ConfigFactory.load()
    val localDevEnv = config.getBoolean("environment.localDev")
    val processFromStart = config.getBoolean("environment.processFromStart")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> (if (localDevEnv) "localhost:9092" else "10.10.100.11:9092"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer-group",
      "auto.offset.reset" -> (if(processFromStart) "earliest" else "latest"),
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val configedTopic = config.getString("kafka.topic")
    val topics = Array(configedTopic)

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf()
      .setAppName("MKKafkaConsumer")
    if(localDevEnv) sparkConf.setMaster("local")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    //Because of updateStateByKey requires this
    ssc.checkpoint("/tmp/log-analyzer-streaming")
    if(localDevEnv)
      ssc.sparkContext.setLogLevel("ERROR")
    else
      ssc.sparkContext.setLogLevel("DEBUG")

    // Create direct kafka stream with brokers and topics
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    //Filter out kafka metadata
    val messages = stream.map(_.value)
    val structuredMessages = messages.transform(rdd=> {
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      //Transform String rdd into structured DataFrame by parsing JSON
      import spark.implicits._
      val ds = spark.createDataset[String](rdd)
      val df = spark.read.json(ds)

      //Check, because sometimes rdd is empty and then next operation has exception
      if (df.columns.contains("t"))
        //Add new column to DataFrame: DateType parsed from timestamp
        df.withColumn("date", to_date(from_unixtime(df("t") / 1000))).rdd
      else
        df.rdd
    })
    //Create State Updating function for mapWithState function
    val stateSpec = StateSpec.function(basicInfoStateUpdate _)

    structuredMessages
      //Create Key from _id and date and prepare for simple count aggregation
      .map(x=>((x.getAs[String]("uid"),x.getAs[String]("_id"),x.getAs[DateType]("date")),1L))
      //Group by "_id x date" key and aggregate count
      .reduceByKey(_ + _)
      //Produce stateful streaming, so when new message come, they are aggregated with previous results and only new stuff should be updated
      //.mapWithState(stateSpec)
      //Debug print
      .print(1000)

    logInfo("start the computation...")
    ssc.start()
    ssc.awaitTermination()
    logInfo("computation done!")
  }

  def basicInfoStateUpdate(key:(String,String),value:Option[(Long)],state:State[Long]): Option[((String,String),Long)] = {
    val v = value.get
    if (state.exists()) {
      // For existing keys
      val currentSet = state.get()
      state.update(currentSet + v)
      Some(key, state.get)
    } else {
      // For new keys
      state.update(v)
      Some(key, v)
    }
  }
}

object SparkSessionSingleton {

  @transient  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}