package com.zhimo.datahub.etl.stage2
import java.sql.Date
import com.zhimo.datahub.common.{ConfigHelper, ParsingHelper, PersistenceHelper, SparkSessionSingleton}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{date_add, datediff, last, min}

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



    import spark.implicits._


    //TODO: parse location


    val cleanedUpData = PersistenceHelper.load(localDevEnv, spark, storage).as[UserLogRecord]
      .map(x => {
        val u_a = ParsingHelper.parseUA(x.u_a)
        val parsedUrl = ParsingHelper.parseUrl(ParsingHelper.decodeUrl(x.url))

        ParsedUserLog(
          x.date,
          x.appId,
          parsedUrl.isWechat,
          x.res,
          u_a.client.device.model.getOrElse("Unknown device"),
          u_a.client.os.family,
          u_a.client.os.family + " " + u_a.client.os.major.getOrElse(""),
          u_a.language,
          u_a.connection,
          u_a.client.userAgent.family,
          x._id,
          x.ip,
          x.uid,
          parsedUrl.url,
          ParsingHelper.decodeUrl(x.action_name)
        )
      })
      .persist()

    val sessions = cleanedUpData
      .map(x => {
        DimensionsWithId(
          x.date,
          x.appId,
          x.isWechat,
          x.resolution,
          x.device,
          x.os,
          x.osVersion,
          x.language,
          x.network,
          x.browser,
          x._id
        )
      })
      .groupBy("date", "id")
      .agg(
        last("appId", ignoreNulls = true).alias("appId"),
        last("isWechat", ignoreNulls = true).alias("isWechat"),
        last("resolution", ignoreNulls = true).alias("resolution"),
        last("device", ignoreNulls = true).alias("device"),
        last("os", ignoreNulls = true).alias("os"),
        last("osVersion", ignoreNulls = true).alias("osVersion"),
        last("language", ignoreNulls = true).alias("language"),
        last("network", ignoreNulls = true).alias("network"),
        last("browser", ignoreNulls = true).alias("browser")
      )
      .distinct()
      .groupBy("date", "appId", "isWechat", "resolution", "device", "os", "osVersion", "language", "network", "browser")
      .count()
      .withColumnRenamed("count", "sessions")
    //PersistenceHelper.saveAndShow(localDevEnv, showResults, sessions.toDF(), config.getEnvironmentString("result.client.sessions"), "date", processFromStart)

    val users = cleanedUpData
      .filter(x => {
        (x.uid != null) && (!x.uid.isEmpty)
      })
      .map(x => {
        DimensionsWithId(
          x.date,
          x.appId,
          x.isWechat,
          x.resolution,
          x.device,
          x.os,
          x.osVersion,
          x.language,
          x.network,
          x.browser,
          x.uid
        )
      })
      .groupBy("date", "id")
      .agg(
        last("appId", ignoreNulls = true).alias("appId"),
        last("isWechat", ignoreNulls = true).alias("isWechat"),
        last("resolution", ignoreNulls = true).alias("resolution"),
        last("device", ignoreNulls = true).alias("device"),
        last("os", ignoreNulls = true).alias("os"),
        last("osVersion", ignoreNulls = true).alias("osVersion"),
        last("language", ignoreNulls = true).alias("language"),
        last("network", ignoreNulls = true).alias("network"),
        last("browser", ignoreNulls = true).alias("browser")
      )
      .distinct()
      .persist()

    val dau = users
      .groupBy("date", "appId", "isWechat", "resolution", "device", "os", "osVersion", "language", "network", "browser")
      .count()
      .withColumnRenamed("count", "dau")
    //PersistenceHelper.saveAndShow(localDevEnv, showResults, dau.toDF(), config.getEnvironmentString("result.client.dau"), "date", processFromStart)

    var usersForXau = users
    for (days <- 1 to 7) {
      usersForXau = usersForXau.union(users
        .withColumn("date", date_add(users.col("date"), days))
      )
    }
    val wau = usersForXau
      .distinct()
      .groupBy("date", "appId", "isWechat", "resolution", "device", "os", "osVersion", "language", "network", "browser")
      .count()
      .withColumnRenamed("count", "wau")
    //PersistenceHelper.saveAndShow(localDevEnv, showResults, wau.toDF(), config.getEnvironmentString("result.client.wau"), "date", processFromStart)

    for (days <- 8 to 30) {
      usersForXau = usersForXau.union(users
        .withColumn("date", date_add(users.col("date"), days))
      )
    }
    val mau = usersForXau
      .distinct()
      .groupBy("date", "appId", "isWechat", "resolution", "device", "os", "osVersion", "language", "network", "browser")
      .count()
      .withColumnRenamed("count", "mau")
    //PersistenceHelper.saveAndShow(localDevEnv, showResults, mau.toDF(), config.getEnvironmentString("result.client.mau"), "date", processFromStart)

    val usersOnly = users
      .drop("appId", "isWechat", "resolution", "device", "os", "osVersion", "language", "network", "browser")
      .persist()

    val firstVisit = usersOnly
      .groupBy("id")
      .agg(min("date").alias("earliestDate"))
      .persist()

    val d1returning = usersOnly
      .join(firstVisit, "id")
      .withColumn("age", datediff($"date", $"earliestDate"))
      .filter(x => {
        x.getAs[Integer]("age") == 1
      })
      .drop("earliestDate","age")
      .groupBy("date")
      .count()
      .withColumnRenamed("count","returning")

    val d1retention = usersOnly
        .groupBy("date")
        .count()
        .withColumnRenamed("count","current")
        .join(d1returning,"date")
        .withColumn("d1retention",$"returning"/$"current")
        .drop("current","returning")
    //PersistenceHelper.saveAndShow(localDevEnv,showResults,d1retention.toDF(),config.getEnvironmentString("result.client.d1retention"),"date",processFromStart)


    val d7returning = usersOnly
      .join(firstVisit, "id")
      .withColumn("age", datediff($"date", $"earliestDate"))
      .filter(x => {
        x.getAs[Integer]("age") == 7
      })
      .drop("earliestDate","age")
      .groupBy("date")
      .count()
      .withColumnRenamed("count","returning")

    val d7retention = usersOnly
      .groupBy("date")
      .count()
      .withColumnRenamed("count","current")
      .join(d7returning,"date")
      .withColumn("d7retention",$"returning"/$"current")
      .drop("current","returning")
    //PersistenceHelper.saveAndShow(localDevEnv,showResults,d7retention.toDF(),config.getEnvironmentString("result.client.d7retention"),"date",processFromStart)


    val d30returning = usersOnly
      .join(firstVisit, "id")
      .withColumn("age", datediff($"date", $"earliestDate"))
      .filter(x => {
        x.getAs[Integer]("age") == 30
      })
      .drop("earliestDate","age")
      .groupBy("date")
      .count()
      .withColumnRenamed("count","returning")

    val d30retention = usersOnly
      .groupBy("date")
      .count()
      .withColumnRenamed("count","current")
      .join(d30returning,"date")
      .withColumn("d30retention",$"returning"/$"current")
      .drop("current","returning")

    d1retention.join(d7retention,Seq("date"),"full").show(1000)
    //PersistenceHelper.saveAndShow(localDevEnv,showResults,d30retention.toDF(),config.getEnvironmentString("result.client.d30retention"),"date",processFromStart)



    spark.stop()
    println("Execution duration " + ((System.nanoTime() - startTime) / 1000000000.0))
  }

}

case class DimensionsWithId(
                             date: Date,
                             appId: Long,
                             isWechat: Boolean,
                             resolution: String,
                             device: String,
                             os: String,
                             osVersion: String,
                             language: String,
                             network: String,
                             browser: String,
                             id: String
                           )

case class ParsedUserLog(
                          date: Date,
                          appId: Long,
                          isWechat: Boolean,
                          resolution: String,
                          device: String,
                          os: String,
                          osVersion: String,
                          language: String,
                          network: String,
                          browser: String,
                          _id: String,
                          ip: String,
                          uid: String,
                          url: String,
                          action_name: String
                        )


final case class UserLogRecord(
                                date: Date,
                                _id: String,
                                //_idn:String,
                                //_idts:String,
                                //_idvc:String,
                                _ref: String,
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
                                t: Long,
                                u_a: String,
                                uid: String,
                                url: String,
                                urlref: String
                                //wma:String

                              )

