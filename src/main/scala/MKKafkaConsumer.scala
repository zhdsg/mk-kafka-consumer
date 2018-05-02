/**
  * Created by raven on 29/03/2018.
  */

import kafka.log.Log
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.sql.Date
import java.util.Properties

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.internal.Logging
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{from_unixtime, to_date}
import org.apache.spark.sql.types._
import scala.util.parsing.json.JSONObject

object MKKafkaConsumer extends Logging {

  @transient  private var kafkaBackupProducer: KafkaProducer[String,String] = null

  def main(args: Array[String]) {
    val config = ConfigFactory.load()
    val localDevEnv = config.getBoolean("environment.localDev")
    val processFromStart = config.getBoolean("environment.processFromStart")
    val permanentStorage = config.getString("environment.permanentStorage")
    val backupKafkaTopic = config.getString("environment.backupKafkaTopic")

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
    ssc.sparkContext.setLogLevel("ERROR")


    // Create direct kafka stream with brokers and topics
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val sparkSession = SparkSessionSingleton.getInstance(ssc.sparkContext.getConf)

    val schema = StructType(
      StructField("date", DateType, true) ::
      StructField("combinedId", StringType, true) ::
      StructField("earliestDate", DateType, true) ::
      StructField("sessionLength", LongType, true) ::
      StructField("sessionCount", LongType, true) :: Nil
    )

    if(processFromStart) {
      saveToPermanentStorage(
        sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema),
        permanentStorage,
        true
      )
    }

    //Filter out kafka metadata
    val messages = stream.map(_.value)
    val structuredMessages = messages
      .transform(rdd=> {
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      //Transform String rdd into structured DataFrame by parsing JSON
      import spark.implicits._
      val ds = spark.createDataset[String](rdd)
      val df = spark.read.json(ds)


      if(rdd.isEmpty() || !df.columns.contains("t") || !df.columns.contains("uid") || !df.columns.contains("uid")){
        df.rdd
      }else{
      df
        .filter(x => {
          (x.getAs[Any]("t") != null) && (x.getAs[String]("uid") != null) && (x.getAs[String]("_id") != null)
        })
        //Add new column to DataFrame: DateType parsed from timestamp
        .withColumn("date", to_date(from_unixtime(df("t") / 1000)))
        .rdd
      }})
      .filter(row => row.length>0)

    val sessionsStateSpec = StateSpec.function(sessionsStateUpdate _)

    val sessions = structuredMessages
        .map(x=>(x.getAs[String]("_id"), (
          x.getAs[String]("uid"),
          Date.valueOf(x.getAs[DateType]("date").toString),
          x.getAs[Long]("t"),
          x.getAs[Long]("t")
        )))
        .reduceByKey((a,b)=>{
          val userId = if (a._1.isEmpty) b._1 else a._1
          val sDate = if(a._2.before(b._2))a._2 else b._2
          val sTimestamp = math.min(a._3,b._3)
          val eTimestamp = math.max(a._4,b._4)
          (userId,sDate,sTimestamp,eTimestamp)
        })
        .mapWithState(sessionsStateSpec)

    //sessions.print(1000)
    val dataPointsStateSpec = StateSpec.function(dataPointsStateUpdate _)

    val dataPoints = sessions
      .map(x=>{
        val combinedId = if(x._2._1.isEmpty) "tmp_"+x._1 else x._2._1
        ((combinedId,x._2._2),(x._2._4-x._2._3,1L))
      })
      .reduceByKey((a,b)=>{
        val sessionTime = a._1 + b._1
        val sessionCount = a._2 + b._2
        (sessionTime,sessionCount)
      })
      .mapWithState(dataPointsStateSpec)

    //dataPoints.print(1000)

    val userAge = dataPoints
      .map(x=>{
        (x._1._1,x._1._2)
      })
      .reduceByKey((a,b)=>{
        if (a.before(b)) a else b
      })

    val sessionsWithAge = dataPoints
        .map(x=>{
          (x._1._1,
            (x._1._2,x._2._1,x._2._2)
          )
        })
        .leftOuterJoin(userAge)
        .map(x=>{(
          x._2._1._1,x._1,
          x._2._2.get,x._2._1._2,x._2._1._3
        )})

    sessionsWithAge
      .foreachRDD(rdd=>{
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        val columns = Seq("date", "combinedId", "earliestDate", "sessionLength","sessionCount")
        val df = spark.createDataFrame(rdd).toDF(columns: _*)
        saveToPermanentStorage(df,permanentStorage)
        produceBackupToKafka(backupKafkaTopic,df.collect())
        spark.sql("SELECT date,combinedId,min(earliestDate) as earliestDate,max(sessionLength) as sessionLength,max(sessionCount) as sessionCount FROM parquet.`"+permanentStorage+"` GROUP BY date, combinedId").show(1000)
      })

    //sessionsWithAge.print(1000)

    logInfo("start the computation...")
    ssc.start()
    ssc.awaitTermination()
    logInfo("computation done!")
  }


  def sessionsStateUpdate(key:String,value:Option[(String,Date,Long,Long)],state:State[(String,Date,Long,Long)]): (String,(String,Date,Long,Long)) = {
    val v = value.get
    if (state.exists()) {
      // For existing keys
      val currentSet = state.get()
      val newSet = (
        if(!currentSet._1.isEmpty) currentSet._1 else v._1,
        if(currentSet._2.before(v._2)) currentSet._2 else v._2,
        math.min(currentSet._3,v._3),
        math.max(currentSet._4,v._4)
      )
      state.update(newSet)
      (key, state.get)
    } else {
      // For new keys
      state.update(v)
      (key, v)
    }
  }

  def dataPointsStateUpdate(key:(String,Date),value:Option[(Long,Long)],state:State[(Long,Long)]): ((String,Date),(Long,Long)) = {
    val v = value.get
    if (state.exists()) {
      // For existing keys
      val currentSet = state.get()
      val newSet = (
        currentSet._1+v._1,
        currentSet._2+v._2
      )
      state.update(newSet)
      (key, state.get)
    } else {
      // For new keys
      state.update(v)
      (key, v)
    }
  }

  def saveToPermanentStorage(dataFrame: DataFrame,file:String,overwrite:Boolean = false): Unit ={
    val writer = dataFrame
      .write
      .format("parquet")
      .partitionBy("date")
    val writerMode = if(overwrite) writer.mode("overwrite") else writer.mode("append")
    writerMode.save(file)
  }

  def produceBackupToKafka(topic:String,messages:Array[Row]): Unit ={
    if(kafkaBackupProducer==null){
      val config = ConfigFactory.load()
      val localDevEnv = config.getBoolean("environment.localDev")
      val kafkaBackupProducerParams = new Properties()
      kafkaBackupProducerParams.put("bootstrap.servers", if (localDevEnv) "localhost:9092" else "10.10.100.11:9092")
      kafkaBackupProducerParams.put("client.id", "MKKafkaConsumer")
      kafkaBackupProducerParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      kafkaBackupProducerParams.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      kafkaBackupProducer = new KafkaProducer[String, String](kafkaBackupProducerParams)
    }
    for(row<-messages) {
      val message = JSONObject(row.getValuesMap(row.schema.fieldNames)).toString()
      kafkaBackupProducer.send(new ProducerRecord[String, String](topic, message))
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