package com.zhimo.datahub.etl.stage2

import java.sql.Date

import com.zhimo.datahub.common._
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{last, sum, countDistinct, date_add, datediff, min, lit}
import scala.collection.mutable.ListBuffer


object MKStage2Client extends Logging {

  def main(args: Array[String]) {
    val startTime = System.nanoTime()
    val config = new ConfigHelper(this)
    val processFromStart = config.getBoolean("processFromStart")
    val localDevEnv = config.getBoolean("localDev")
    val showResults = config.getBoolean("showResults")

    // Template: Specify permanent storage Parquet file
    val storage = config.getEnvironmentString("storage.client")


    val sparkConf = new SparkConf()
      .setAppName("MKKafkaConsumer")
    if (localDevEnv) {
      sparkConf.setMaster("local")
    } else {
      sparkConf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")
    }

    val spark = SparkSessionSingleton.getInstance(sparkConf, !localDevEnv)

    if (localDevEnv) {
      spark.sparkContext.setLogLevel("ERROR")
    }

    println("Before analysis " + ((System.nanoTime() - startTime) / 1000000000.0))

    Geo2IPHelper.init(localDevEnv,spark,config)

    println("Geo Helper Inited " + ((System.nanoTime() - startTime) / 1000000000.0))

    import spark.implicits._

    //TODO: parse location



    val cleanedUpData = PersistenceHelper.loadFromParquet(spark, storage)
      .as[UserLogRecord]
      .map(x => {
        val parsedUrl = ParsingHelper.parseUrl(ParsingHelper.decodeUrl(x.url))

        val uid = if (x.uid == null || x.uid.isEmpty) x._id else x.uid

        ParsedUserLog(
          x.date,
          x.appId,
          parsedUrl.isWechat,
          x.res,
          x._id,
          x.ip,
          uid,
          x.u_a
        )
      })
      .persist()

    println("Cleaned up data " + cleanedUpData.count() + " " + ((System.nanoTime() - startTime) / 1000000000.0))

    val users = cleanedUpData
      .groupBy("date", "uid", "_id")
      .agg(
        last("appId", ignoreNulls = true).alias("appId"),
        last("isWechat", ignoreNulls = true).alias("isWechat"),
        last("resolution", ignoreNulls = true).alias("resolution"),
        last("u_a", ignoreNulls = true).alias("u_a"),
        last("locId", ignoreNulls = true).alias("locId")
      )
      .as[ParsedUserLog]
      .map(x=>{
        val locId = Geo2IPHelper.ip2LocId(x.locId)
        ParsedUserLog(
          x.date,
          x.appId,
          x.isWechat,
          x.resolution,
          x._id,
          Geo2IPHelper.getLocation(locId),
          x.uid,
          x.u_a
        )
      })
      .persist()
    
    println("Users " + users.count() + " " + ((System.nanoTime() - startTime) / 1000000000.0))

    val usersForBasics = users
      .flatMap(x => {
        val lst = ListBuffer(
          DimensionsAndIDs(x.date, x.appId, x.isWechat, x.resolution, x.locId, x.u_a, x._id, x.uid, x.uid, x.uid)
        )
        for (days <- 1 to 30) {
          if (days <= 7) {
            val y = DimensionsAndIDs(new Date(x.date.getTime + (days * 1000 * 60 * 60 * 24)), x.appId, x.isWechat, x.resolution, x.locId, x.u_a, null, null, x.uid, x.uid)
            lst += y
          } else {
            val y = DimensionsAndIDs(new Date(x.date.getTime + (days * 1000 * 60 * 60 * 24)), x.appId, x.isWechat, x.resolution, x.locId, x.u_a, null, null, null, x.uid)
            lst += y
          }
        }
        lst
      })

    val basics = usersForBasics
      .groupBy("date", "appId", "isWechat", "resolution", "u_a", "locId")
      .agg(
        countDistinct("sessionsIDs").alias("sessions"),
        countDistinct("dauIDs").alias("dau"),
        countDistinct("wauIDs").alias("wau"),
        countDistinct("mauIDs").alias("mau")
      )
      .as[DimensionsMetricsWithoutUA]
      .map(x => {
        val u_a = ParsingHelper.parseUA(x.u_a)
        DimensionsMetrics(
          x.date,
          x.appId,
          x.isWechat,
          x.resolution,
          x.locId,
          u_a.client.device.model.getOrElse("Unknown device"),
          u_a.client.os.family,
          u_a.client.os.family + " " + u_a.client.os.major.getOrElse(""),
          u_a.language,
          u_a.connection,
          u_a.client.userAgent.family,
          x.sessions,
          x.dau,
          x.wau,
          x.mau
        )
      })
      .groupBy("date", "appId", "isWechat", "resolution", "device", "os", "osVersion", "language", "network", "browser","locId")
      .agg(
        sum("sessions").alias("sessions"),
        sum("dau").alias("dau"),
        sum("wau").alias("wau"),
        sum("mau").alias("mau")
      )
    PersistenceHelper.saveAndShow(localDevEnv, showResults, basics.toDF(), config.getEnvironmentString("result.client.basics"), null, processFromStart)

    val usersOnly = users
      .select("date", "uid")
      .persist()

    println("Users only " + usersOnly.count() + " " + ((System.nanoTime() - startTime) / 1000000000.0))

    val firstVisit = usersOnly
      .groupBy("uid")
      .agg(min("date").alias("earliestDate"))
      .persist()

    println("First Visit " + firstVisit.count() + " " + ((System.nanoTime() - startTime) / 1000000000.0))

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


    spark.stop()
    println("Execution duration " + ((System.nanoTime() - startTime) / 1000000000.0))
  }

}

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

case class DimensionsAndIDs(
                             date: Date,
                             appId: Long,
                             isWechat: Boolean,
                             resolution: String,
                             locId: String,
                             u_a: String,
                             sessionsIDs: String,
                             dauIDs: String,
                             wauIDs: String,
                             mauIDs: String
                           )

case class DimensionsMetricsWithoutUA(
                                       date: Date,
                                       appId: Long,
                                       isWechat: Boolean,
                                       resolution: String,
                                       locId: String,
                                       u_a: String,
                                       sessions: String,
                                       dau: Long,
                                       wau: Long,
                                       mau: Long
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
                              sessions: String,
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
                                //e_a: String,
                                //e_c: String,
                                //e_n: String,
                                //e_v:String,
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

