/**
  * Created by raven on 29/03/2018.
  */


import java.sql.Date
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{from_unixtime, to_date,date_add,datediff}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object MKBasicMetricsConsumer extends Logging {


  def main(args: Array[String]) {
    val config = new ConfigHelper(this)
    val localDevEnv = config.getBoolean("localDev")
    val processFromStart = config.getBoolean("processFromStart")
    val permanentStorage = config.getEnvironmentString("permanentStorage")
    val hiveStorage = config.getEnvironmentString("hiveStorage")
    val backupKafkaTopic = config.getEnvironmentString("kafka.backupKafkaTopic")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> (if (localDevEnv) config.getString("kafka.server_localDev") else config.getString("kafka.server")),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer-group",
      "auto.offset.reset" -> (if(processFromStart) "earliest" else "latest"),
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val configuredTopic = config.getEnvironmentString("kafka.topic")
    val topics = Array(configuredTopic)

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf()
      .setAppName("MKKafkaConsumer")
      if(!localDevEnv) {
        sparkConf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")
      }
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

    val sparkSession = SparkSessionSingleton.getInstance(ssc.sparkContext.getConf,!localDevEnv)

    val schema = StructType(
      StructField("date", DateType, nullable = true) ::
      StructField("combinedId", StringType, nullable = true) ::
      StructField("earliestDate", DateType, nullable = true) ::
      StructField("sessionLength", LongType, nullable = true) ::
      StructField("sessionCount", LongType, nullable = true) :: Nil
    )

    if(processFromStart) {
      PersistenceHelper.saveToParquetStorage(
        sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema),
        permanentStorage,
        "date",
        overwrite = true
      )
    }

    //Filter out kafka metadata
    val messages = stream.map(_.value)
    messages.print(10)

    val structuredMessages = messages
      .transform(rdd=> {
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf,!localDevEnv)
      //Transform String rdd into structured DataFrame by parsing JSON
      import spark.implicits._
      val ds = spark.createDataset[String](rdd)
      val df = spark.read.json(ds)


      if(rdd.isEmpty() || !df.columns.contains("t") || !df.columns.contains("uid") || !df.columns.contains("_id")){
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

    structuredMessages.print(10)

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

    sessions.print(1000)

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

    dataPoints.print(1000)

    val userAge = dataPoints
      .map(x=>{
        (x._1._1,x._1._2)
      })
      .reduceByKey((a,b)=>{
        if (a.before(b)) a else b
      })

    userAge.print(1000)

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
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf,!localDevEnv)
        val columns = Seq("date", "combinedId", "earliestDate", "sessionLength","sessionCount")
        val df = spark.createDataFrame(rdd).toDF(columns: _*)
        PersistenceHelper.saveToParquetStorage(df,permanentStorage,"date")
        PersistenceHelper.saveToHive(df,hiveStorage,"date")
        KafkaBackupProducerHelper.produce(backupKafkaTopic,df.take(df.count.toInt))
        spark.sql("SELECT date,combinedId,min(earliestDate) as earliestDate,max(sessionLength) as sessionLength,max(sessionCount) as sessionCount FROM parquet.`"+permanentStorage+"` GROUP BY date, combinedId").show(1000)
      })

    sessionsWithAge.print(1000)

    val newUsers = sessionsWithAge
        .filter(x=>{
          x._1 == x._3
        })
        .map(x=>{
          (x._2,(x._1,1))
        })
        .reduceByKey((a,b)=>{
          (a._1,a._2+b._2)
        })
        .map(x=>{
          (x._2._1,x._2._2)
        })
    newUsers.print(1000)

    val dau = sessionsWithAge
        .map(x=>{
          ((x._1,x._2),1)
        })
        .reduceByKey((a,b)=>{
          a+b
        })
        .map(x=>{
          (x._1._1,x._2)
        })
    dau.print(1000)

    val wau = sessionsWithAge
      .map(x=>{
          (x._1,x._2)
        })
      .transform(rdd=>{
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf,!localDevEnv)
        val columns = Seq("date", "combinedId")
        val df = spark.createDataFrame(rdd).toDF(columns: _*)
        var df2 = df
        for(days <- 1 to 7) {
          df2 = df2.union(df
            .withColumn("date", date_add(df.col("date"), days))
          )
        }
        df2.rdd
      })
      .map(x=>{
        (x.getAs[Date]("date"),1)
      })
      .reduceByKey((a,b)=>{
        a+b
      })
    wau.print(1000)

    val mau = sessionsWithAge
      .map(x=>{
        (x._1,x._2)
      })
      .transform(rdd=>{
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf,!localDevEnv)
        val columns = Seq("date", "combinedId")
        val df = spark.createDataFrame(rdd).toDF(columns: _*)
        var df2 = df
        for(days <- 1 to 30) {
          df2 = df2.union(df
            .withColumn("date", date_add(df.col("date"), days))
          )
        }
        df2.rdd
      })
      .map(x=>{
        (x.getAs[Date]("date"),1)
      })
      .reduceByKey((a,b)=>{
        a+b
      })
    mau.print(1000)

    val d1retention = sessionsWithAge
      .map(x=>{
        (x._1,x._2,x._3)
      })
      .transform(rdd=>{
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf,!localDevEnv)
        val columns = Seq("date", "combinedId","age")
        val df = spark.createDataFrame(rdd).toDF(columns: _*)
        df
          .withColumn("age",datediff(df.col("date"),df.col("age")))
          .rdd
      })
      .filter(x=>{
        x.getAs[Integer]("age") == 1
      })
      .map(x=>{
        (x.getAs[String]("date"),1)
      })
      .reduceByKey((a,b)=>{
        a+b
      })
    d1retention.print(1000)

    val d7retention = sessionsWithAge
      .map(x=>{
        (x._1,x._2,x._3)
      })
      .transform(rdd=>{
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf,!localDevEnv)
        val columns = Seq("date", "combinedId","age")
        val df = spark.createDataFrame(rdd).toDF(columns: _*)
        df
          .withColumn("age",datediff(df.col("date"),df.col("age")))
          .rdd
      })
      .filter(x=>{
        x.getAs[Integer]("age") == 7
      })
      .map(x=>{
        (x.getAs[String]("date"),1)
      })
      .reduceByKey((a,b)=>{
        a+b
      })
    d7retention.print(1000)

    val d30retention = sessionsWithAge
      .map(x=>{
        (x._1,x._2,x._3)
      })
      .transform(rdd=>{
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf,!localDevEnv)
        val columns = Seq("date", "combinedId","age")
        val df = spark.createDataFrame(rdd).toDF(columns: _*)
        df
          .withColumn("age",datediff(df.col("date"),df.col("age")))
          .rdd
      })
      .filter(x=>{
        x.getAs[Integer]("age") == 30
      })
      .map(x=>{
        (x.getAs[String]("date"),1)
      })
      .reduceByKey((a,b)=>{
        a+b
      })
    d30retention.print(1000)



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


}