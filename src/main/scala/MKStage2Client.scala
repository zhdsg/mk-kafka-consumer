import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import java.sql.Date
import org.apache.spark.sql.functions.monotonically_increasing_id

object MKStage2Client extends Logging {

  def main(args: Array[String]) {
    val config = new ConfigHelper(this)
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

    import spark.implicits._

    println(ParsingHelper.parseUA(
      "Mozilla/5.0 (iPhone; CPU iPhone OS 11_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E216 MicroMessenger/6.6.5 NetType/4G Language/zh_CN"
    ))
    println(ParsingHelper.parseUA(
      "Mozilla/5.0 (Linux; Android 7.1.2; Redmi 4X Build/N2G47H; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/57.0.2987.132 MQQBrowser/6.2 TBS/044005 Mobile Safari/537.36 MicroMessenger/6.6.5.1280(0x26060536) NetType/WIFI Language/zh_CN"
    ))

    //DONE: parse language from user agent
    //DONE: parse network type from user agent
    //DONE: better parse url
    //TODO: parse location
    //TODO: compile the dimension table
    //TODO: compile the fact table
    //TODO: compile measures for dimension table


    val cleanedUpData = PersistenceHelper.load(localDevEnv,spark,storage).as[UserLogRecord]
      .map(x => {
        UserLogRecord(
          x.date,
          x._id,
          //x._idn, x._idts, x._idvc,
          ParsingHelper.decodeUrl(x._ref),
          //x._refts, x._viewts,
          ParsingHelper.decodeUrl(x.action_name),
          //x.ag,
          x.appId,
          //x.cookie, x.data, x.dir,
          x.e_a,
          x.e_c,
          ParsingHelper.decodeUrl(x.e_n),
          //x.e_v, x.fla, x.gears, x.gt_ms,
          x.ip,
          //x.java, x.pdf, x.pv_id, x.qt, x.r, x.realp, x.rec,
          x.res,
          x.t,
          x.u_a,
          x.uid,
          ParsingHelper.decodeUrl(x.url),
          ParsingHelper.decodeUrl(x.urlref)
          //x.wma
        )
      })

    val dimensions = cleanedUpData
        .map(x=>{
          val u_a = ParsingHelper.parseUA(x.u_a)
          val parsedUrl = ParsingHelper.parseUrl(x.url)

          Dimension(
            0,
            x.date,
            x.appId,
            parsedUrl.url,
            parsedUrl.isWechat,
            x.res,
            u_a.client.device.model.getOrElse("Unknown device"),
            u_a.client.os.family,
            u_a.client.os.family+" "+u_a.client.os.major.getOrElse(""),
            u_a.language,
            u_a.connection,
            x.action_name
          )
        })
        .distinct()
        .withColumn("dim_id",monotonically_increasing_id())


    dimensions.show(1000)

    spark.stop()
  }

}

final case class Dimension(
                             dim_id:Long,
                             date:Date,
                             appId:Long,
                             url:String,
                             isWechat:Boolean,
                             resolution:String,
                             device:String,
                             os:String,
                             osVersion:String,
                             language:String,
                             network:String,
                             actionName:String
                          )

final case class SessionFact(
                            dim_fk:Long,
                            userId:String,
                            sessionId:String
                            )

final case class UserLogRecord(
                                date:Date,
                                _id:String,
                                //_idn:String,
                                //_idts:String,
                                //_idvc:String,
                                _ref:String,
                                //_refts:Long,
                                //_viewts:String,
                                action_name:String,
                                //ag:String,
                                appId:Long,
                                //cookie:String,
                                //data:String,
                                //dir:String,
                                e_a:String,
                                e_c:String,
                                e_n:String,
                                //e_v:String,
                                //fla:String,
                                //gears:String,
                                //gt_ms:Long,
                                ip:String,
                                //java:String,
                                //pdf:String,
                                //pv_id:String,
                                //qt:String,
                                //r:String,
                                //realp:String,
                                //rec:Long,
                                res:String,
                                t:Long,
                                u_a:String,
                                uid:String,
                                url:String,
                                urlref:String
                                //wma:String

                              )

