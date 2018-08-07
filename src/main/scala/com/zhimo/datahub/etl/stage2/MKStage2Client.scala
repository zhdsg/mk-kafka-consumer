package com.zhimo.datahub.etl.stage2

import java.sql.Date

import com.zhimo.datahub.common._
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{collect_list, countDistinct, datediff, last, min, expr , max, date_add,lit,desc}
import org.apache.spark.storage.StorageLevel
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import java.nio.file.{Paths, Files}

object MKStage2Client extends Logging {

  def main(args: Array[String]) {
    val startTime = System.nanoTime()
    val config = new ConfigHelper(this)
    val processFromStart = config.getBoolean("processFromStart")
    val localDevEnv = config.getBoolean("localDev")
    val showResults = config.getBoolean("showResults")

    // Template: Specify permanent storage Parquet file
    val storage = config.getEnvironmentString("storage.client")

    var processedUntil = new Date(0L)
    val pathToProcessedUntil = Paths.get(config.getString("processedUntilFile"))
    if(Files.exists(pathToProcessedUntil)){
      val lines = Files.readAllLines(pathToProcessedUntil)
      if(lines.size()>0){
        processedUntil = Date.valueOf(lines.get(0))
      }
    }

    println(processedUntil.toString)

    val sparkConf = new SparkConf()
      .setAppName(ConfigHelper.getClassName(this))
    if (localDevEnv) {
      sparkConf.setMaster("local")
    } else {
      sparkConf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")
    }

    val spark = SparkSessionSingleton.getInstance(sparkConf, !localDevEnv)

    if (localDevEnv) {
      spark.sparkContext.setLogLevel("ERROR")
    } else {
      spark.sparkContext.setLogLevel("WARN")
    }

    println("Before analysis " + ((System.nanoTime() - startTime) / 1000000000.0))

    val geo = spark.sparkContext.broadcast(Geo2IPHelper.init(localDevEnv, spark))

    println("Geo Helper Inited " + ((System.nanoTime() - startTime) / 1000000000.0))

    import spark.implicits._


    val cleanedUpData = PersistenceHelper.loadFromParquet(spark, storage)
      .as[UserLogRecord]
      .map(x => {
        val parsedUrl = ParsingHelper.parseUrl(ParsingHelper.decodeUrl(x.url))

        (ParsedUserLog(
          x.date,
          x.appId,
          parsedUrl.isWechat,
          x.res,
          x._id,
          x.ip,
          x.uid,
          x.u_a
        ), FunnelData(
          if(x.uid == null || x.uid.isEmpty) x._id else x.uid,
          //x.e_a,
          parsedUrl.urlWithoutIds,
          ""//,
          //ParsingHelper.decodeUrl(x.e_n),
          //x.e_v
        ))
      })

    val funnelAggregation = cleanedUpData
      .map(x => {
        x._2
      })
      .filter(x=>{
        (x.e_a!=null)//||(x.e_c!=null)//||(!x.e_n.isEmpty)||(x.e_v!=null)
      })
      .groupBy("uid", "e_a", "e_c"/*, "e_n", "e_v"*/)
      .count()
      .drop("count")
      .persist(StorageLevel.MEMORY_AND_DISK)

    println("Funnel Aggregation " + ((System.nanoTime() - startTime) / 1000000000.0))



    val funnelCounts = funnelAggregation
      .groupBy("e_c","e_a")
      .count()
      .drop("e_c")
      .orderBy("count")
      .as[FunnelCount]
      .collectAsList()

    var funnelOrder = Map[String,Long]()
    var funnelPrint = ""
    for(x <- funnelCounts.asScala){
      funnelPrint+=x.e_a+":"+x.count+", "
      funnelOrder += (x.e_a -> x.count)
    }
    println(funnelPrint)

    val funnel = funnelAggregation
      .groupBy("uid","e_c")
      .agg(
        collect_list("e_a").alias("funnelsteps")
      )
      .as[FunnelEntry]
      .map(x=>{
        val steps = x.funnelsteps.sortWith((a,b)=>{
          funnelOrder.getOrElse(a,0L)>funnelOrder.getOrElse(b,0L)
        })
        FunnelEntry(
          x.e_c,
          steps,
          x.uid
        )
      })
      .groupBy("e_c","funnelsteps")
      .agg(
        countDistinct("uid").alias("count")
      ).orderBy(desc("count"))

    PersistenceHelper.saveAndShow(localDevEnv,showResults,funnel,config.getEnvironmentString("result.client.funnels"),null,processFromStart)

    val users = cleanedUpData
      .map(x => {
        x._1
      })
      .groupBy("date", "uid", "_id")
      .agg(
        last("appId", ignoreNulls = true).alias("appId"),
        last("isWechat", ignoreNulls = true).alias("isWechat"),
        last("resolution", ignoreNulls = true).alias("resolution"),
        last("u_a", ignoreNulls = true).alias("u_a"),
        last("locId", ignoreNulls = true).alias("locId")
      )
      .as[ParsedUserLog]
      .map(x => {
        val locId = Geo2IPHelper.ip2LocId(x.locId)
        val u_a = ParsingHelper.parseUA(x.u_a)
        ParsedUserLogParsedUA(
          x.date,
          x.appId,
          x.isWechat,
          x.resolution,
          x._id,
          Geo2IPHelper.getLocation(locId, geo),
          x.uid,
          u_a.client.device.model.getOrElse("Unknown device"),
          u_a.client.os.family,
          u_a.client.os.family + " " + u_a.client.os.major.getOrElse(""),
          u_a.language,
          u_a.connection,
          u_a.client.userAgent.family
        )
      })
      .persist(StorageLevel.MEMORY_AND_DISK)

    println("Users " + ((System.nanoTime() - startTime) / 1000000000.0))

    val processedUntilArray = users.select("date").agg(max("date").as("date")).withColumn("date",date_add($"date",-1)).collectAsList()
    if(processedUntilArray.size>0){
      processedUntil = Date.valueOf(processedUntilArray.get(0).getAs[Date]("date").toString)
    }

    println(processedUntil.toString)

    val usersForBasics = users
      .flatMap(x => {
        val lst = ListBuffer(
          DimensionsAndIDs(x.date, x.appId, x.isWechat, x.resolution, x.locId, x.device, x.os, x.osVersion, x.language, x.network, x.browser, x._id, x.uid, x.uid, x.uid)
        )
        for (days <- 1L to 30L) {
          if (days <= 7L) {
            val y = DimensionsAndIDs(new Date(x.date.getTime + (days * 1000L * 60L * 60L * 24L)), x.appId, x.isWechat, x.resolution, x.locId, x.device, x.os, x.osVersion, x.language, x.network, x.browser, null, null, x.uid, x.uid)
            lst += y
          } else {
            val y = DimensionsAndIDs(new Date(x.date.getTime + (days * 1000L * 60L * 60L * 24L)), x.appId, x.isWechat, x.resolution, x.locId, x.device, x.os, x.osVersion, x.language, x.network, x.browser, null, null, null, x.uid)
            lst += y
          }
        }
        lst
      })
      .persist(StorageLevel.MEMORY_AND_DISK)

    println("Users for basics " + ((System.nanoTime() - startTime) / 1000000000.0))

    val basics = usersForBasics
      .groupBy("date", "appId", "isWechat", "resolution", "device", "os", "osVersion", "language", "network", "browser", "locId")
      .agg(
        countDistinct("sessionsIDs").alias("sessions"),
        countDistinct("dauIDs").alias("dau"),
        countDistinct("wauIDs").alias("wau"),
        countDistinct("mauIDs").alias("mau")
      )
      .as[DimensionsMetrics]

    PersistenceHelper.saveAndShow(localDevEnv, showResults, basics.toDF(), config.getEnvironmentString("result.client.basics"), null, processFromStart)
    println("Basics saved " + ((System.nanoTime() - startTime) / 1000000000.0))
    

    val usersOnly = users
      .select("date", "uid")
      .persist(StorageLevel.MEMORY_AND_DISK)

    println("Users only " + ((System.nanoTime() - startTime) / 1000000000.0))

    val firstVisit = usersOnly
      .groupBy("uid")
      .agg(min("date").alias("earliestDate"))
      .persist(StorageLevel.MEMORY_AND_DISK)

    println("First Visit " + ((System.nanoTime() - startTime) / 1000000000.0))

    val d1returning = usersOnly
      .join(firstVisit, "uid")
      .withColumn("age", datediff($"date", $"earliestDate"))
      .filter(x => {
        x.getAs[Integer]("age") == 1
      })
      .drop("earliestDate", "age")
      .groupBy("date")
      .count()
      .withColumnRenamed("count", "returning")

    val d1retention = usersOnly
      .groupBy("date")
      .count()
      .withColumnRenamed("count", "current")
      .join(d1returning, "date")
      .withColumn("d1retention", $"returning" / $"current")
      .drop("current", "returning")

    val d7returning = usersOnly
      .join(firstVisit, "uid")
      .withColumn("age", datediff($"date", $"earliestDate"))
      .filter(x => {
        x.getAs[Integer]("age") == 7
      })
      .drop("earliestDate", "age")
      .groupBy("date")
      .count()
      .withColumnRenamed("count", "returning")

    val d7retention = usersOnly
      .groupBy("date")
      .count()
      .withColumnRenamed("count", "current")
      .join(d7returning, "date")
      .withColumn("d7retention", $"returning" / $"current")
      .drop("current", "returning")

    val d30returning = usersOnly
      .join(firstVisit, "uid")
      .withColumn("age", datediff($"date", $"earliestDate"))
      .filter(x => {
        x.getAs[Integer]("age") == 30
      })
      .drop("earliestDate", "age")
      .groupBy("date")
      .count()
      .withColumnRenamed("count", "returning")

    val d30retention = usersOnly
      .groupBy("date")
      .count()
      .withColumnRenamed("count", "current")
      .join(d30returning, "date")
      .withColumn("d30retention", $"returning" / $"current")
      .drop("current", "returning")

    val retentions = d1retention
      .join(d7retention, Seq("date"), "full")
      .join(d30retention, Seq("date"), "full")


    PersistenceHelper.saveAndShow(localDevEnv, showResults, retentions.toDF(), config.getEnvironmentString("result.client.retentions"), null, processFromStart)

//    println("Computation duration " + ((System.nanoTime() - startTime) / 1000000000.0))
//    Thread.sleep(10000000L)

    spark.stop()
    println("Execution duration " + ((System.nanoTime() - startTime) / 1000000000.0))
  }

}

case class FunnelCount(
  e_a:String,
  count:Long
               )

case class ParsedUserLog(
                          date: Date,
                          appId: Long,
                          isWechat: Boolean,
                          resolution: String,
                          _id: String,
                          locId: String,
                          uid: String,
                          u_a: String
                        )

case class ParsedUserLogParsedUA(
                          date: Date,
                          appId: Long,
                          isWechat: Boolean,
                          resolution: String,
                          _id: String,
                          locId: String,
                          uid: String,
                          device: String,
                          os: String,
                          osVersion: String,
                          language: String,
                          network: String,
                          browser: String
                        )

case class DimensionsAndIDs(
                             date: Date,
                             appId: Long,
                             isWechat: Boolean,
                             resolution: String,
                             locId: String,
                             device: String,
                             os: String,
                             osVersion: String,
                             language: String,
                             network: String,
                             browser: String,
                             sessionsIDs: String,
                             dauIDs: String,
                             wauIDs: String,
                             mauIDs: String
                           )

case class DimensionsMetrics(
                              date: Date,
                              appId: Long,
                              isWechat: Boolean,
                              resolution: String,
                              locId: String,
                              device: String,
                              os: String,
                              osVersion: String,
                              language: String,
                              network: String,
                              browser: String,
                              sessions: Long,
                              dau: Long,
                              wau: Long,
                              mau: Long
                            )

final case class UserLogRecord(
                                date: Date,
                                _id: String,
                                //_idn:String,
                                //_idts:String,
                                //_idvc:String,
                                //_ref: String,
                                //_refts:Long,
                                //_viewts:String,
                                action_name: String,
                                //ag:String,
                                appId: Long,
                                //cookie:String,
                                //data:String,
                                //dir:String,
                                e_a: String,
                                e_c: String,
                                e_n: String,
                                e_v: String,
                                //fla:String,
                                //gears:String,
                                //gt_ms:Long,
                                ip: String,
                                //java:String,
                                //pdf:String,
                                //pv_id:String,
                                //qt:String,
                                //r:String,
                                //realp:String,
                                //rec:Long,
                                res: String,
                                //t: Long,
                                u_a: String,
                                uid: String,
                                url: String
                                //urlref: String
                                //wma:String
                              )

final case class FunnelData(
                             uid: String,
                             e_a: String,
                             e_c: String//,
//                             e_n: String,
//                             e_v: String
                           )

final case class FunnelEntry(
                            e_c:String,
                            funnelsteps:List[String],
                            uid:String
                            )

final case class FunnelResult(
                              e_c:String,
                              funnelsteps:List[String],
                              count:Long
                            )