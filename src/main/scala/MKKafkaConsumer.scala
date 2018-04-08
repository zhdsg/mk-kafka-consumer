/**
  * Created by raven on 29/03/2018.
  */

import java.util.Properties

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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MKKafkaConsumer extends Logging {
  def main(args: Array[String]) {
    // JDBC writer configuration
    val connectionProperties = new Properties()
    connectionProperties.put("user", "xubao")
    connectionProperties.put("password", "8xd8IiHR")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.10.100.11:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val configedTopic = ConfigFactory.load().getString("kafka.topic")
    val topics = Array(configedTopic)

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf()
      .setAppName("MKKafkaConsumer")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
//    ssc.sparkContext.setLogLevel("DEBUG")

    // Create direct kafka stream with brokers and topics
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val lines = stream.map(_.value())
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()


    words.foreachRDD { rdd=>
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._
      val structuredData = rdd.map(x => Record(x))
      val df = structuredData.toDF() // create a dataframe from the schema RDD
      df.write.mode("append")
        .jdbc("jdbc:mysql://10.10.100.2:3306/erpstatsdev?useUnicode=true&characterEncoding=UTF-8", "spark_test", connectionProperties)
    }

    logInfo("start the computation...")
    ssc.start()
    ssc.awaitTermination()
    logInfo("computation done!")
  }
}
/** Case class for converting RDD to DataFrame */
case class Record(word: String)


/** Lazily instantiated singleton instance of SparkSession */
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
