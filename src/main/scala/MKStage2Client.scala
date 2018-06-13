import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import java.sql.Date
import org.apache.spark.sql.functions.last

object MKStage2Client extends Logging {

  def main(args: Array[String]) {
    val startTime = System.nanoTime()
    val config = new ConfigHelper(this)
    val processFromStart = config.getBoolean("processFromStart")
    val localDevEnv = config.getBoolean("localDev")

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
    sessions.show(1000)
    PersistenceHelper.save(localDevEnv, sessions.toDF(), config.getEnvironmentString("result.client.sessions"), "date", processFromStart)

    val dau = cleanedUpData
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
      .groupBy("date", "appId", "isWechat", "resolution", "device", "os", "osVersion", "language", "network", "browser")
      .count()
      .withColumnRenamed("count", "dau")
    dau.show(1000)
    PersistenceHelper.save(localDevEnv, dau.toDF(), config.getEnvironmentString("result.client.dau"), "date", processFromStart)


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

