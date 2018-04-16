/**
  * Created by raven on 29/03/2018.
  */

import kafka.log.Log
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.sql.Date
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
    val finalStateSpec = StateSpec.function(basicInfoStateUpdate _)
    val connectIDsStateSpec = StateSpec.function(iDsConnectStateUpdate _)

    val idMapping = structuredMessages
      .map(x=>(x.getAs[String]("_id"), (x.getAs[String]("uid"),Date.valueOf(x.getAs[DateType]("date").toString))))
      .reduceByKey((a,b)=>{
        val id = if (!a._1.isEmpty) a._1 else if (!b._1.isEmpty) b._1 else ""
        val date = if (a._2.before(b._2)) a._2 else b._2
        (id,date)
      })
      .filter(x=> !x._2._1.isEmpty)
      //.mapWithState(connectIDsStateSpec)

    idMapping.print(1000)

    structuredMessages
      .map(x=>(x.getAs[String]("_id"),( x.getAs[String]("uid"),x.getAs[DateType]("date"))))
      .leftOuterJoin(idMapping)
      .map(x=>{
        val id = if(!x._2._1._1.isEmpty) x._2._1 else if (x._2._2.isDefined) (x._2._2.get._1,x._2._1._2) else ("tmp_"+x._1,x._2._1._2)
        val earliestDate = if(x._2._2.isDefined) x._2._2.get._2 else Date.valueOf(x._2._1._2.toString)
        (id,earliestDate)
      })
      .reduceByKey((a,b)=> if (a.before(b)) a else b)
      //Produce stateful streaming, so when new message come, they are aggregated with previous results and only new stuff should be updated
      .mapWithState(finalStateSpec)
      //Debug print
      .print(1000)

    logInfo("start the computation...")
    ssc.start()
    ssc.awaitTermination()
    logInfo("computation done!")
  }

  def basicInfoStateUpdate(key:(String,DateType),value:Option[(Date)],state:State[Date]): ((String,DateType),Date) = {
    val v = value.get
    if (state.exists()) {
      // For existing keys
      val currentSet = state.get()
      state.update(if(currentSet.before(v)) currentSet else v)
      (key, state.get)
    } else {
      // For new keys
      state.update(v)
      (key, v)
    }
  }

  def iDsConnectStateUpdate(key:String,value:Option[(String,Date)],state:State[(String,Date)]): (String,(String,Date)) = {
    val v = value.get
    if (state.exists()) {
      // For existing keys
      val currentSet = state.get()
      val newSet = (
      if(!currentSet._1.isEmpty) currentSet._1 else v._1,
      if(currentSet._2.before(v._2)) currentSet._2 else v._2
      )
      state.update(newSet)
      (key, state.get)
    } else {
      // For new keys
      state.update(v)
      (key, v)
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