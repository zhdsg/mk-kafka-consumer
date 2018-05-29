
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import java.sql.Date
import java.net.URLDecoder
import org.uaparser.scala.Parser

object MKStage2Client extends Logging {

  def decodeUrl(str:String): String ={
    URLDecoder.decode(if(str!=null)str else "","utf-8")
  }

  def main(args: Array[String]) {
    val config = new ConfigHelper(this)
    val localDevEnv = config.getBoolean("localDev")

    // Template: Specify permanent storage Parquet file
    val permanentStorage = config.getEnvironmentString("permanentStorage")


    val sparkConf = new SparkConf()
      .setAppName("MKKafkaConsumer")
    if (localDevEnv) {
      sparkConf.setMaster("local")
    } else {
      sparkConf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")
    }

    val spark = SparkSessionSingleton.getInstance(sparkConf, !localDevEnv)

    import spark.implicits._

    println(Parser.get.parse(
      "Mozilla/5.0 (iPhone; CPU iPhone OS 11_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E216 MicroMessenger/6.6.5 NetType/4G Language/zh_CN"
    ))
    println(Parser.get.parse(
      "Mozilla/5.0 (Linux; Android 7.1.2; Redmi 4X Build/N2G47H; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/57.0.2987.132 MQQBrowser/6.2 TBS/044005 Mobile Safari/537.36 MicroMessenger/6.6.5.1280(0x26060536) NetType/WIFI Language/zh_CN"
    ))


    val cleanedUpData = spark.read.parquet(permanentStorage).as[UserLogRecord]
      .map(x => {
        UserLogRecord(
          x.date,
          x._id,
          //x._idn, x._idts, x._idvc,
          decodeUrl(x._ref),
          //x._refts, x._viewts,
          decodeUrl(x.action_name),
          //x.ag,
          x.appId,
          //x.cookie, x.data, x.dir,
          x.e_a,
          x.e_c,
          decodeUrl(x.e_n),
          //x.e_v, x.fla, x.gears, x.gt_ms,
          x.ip,
          //x.java, x.pdf, x.pv_id, x.qt, x.r, x.realp, x.rec,
          x.res,
          x.t,
          x.u_a,
          x.uid,
          decodeUrl(x.url),
          decodeUrl(x.urlref)
          //x.wma
        )
      })

    val dimensions = cleanedUpData
        .map(x=>{

          Dimensions(
            x.date,
            x.appId,
            x.action_name,
            x.e_a,
            x.e_c,
            x.e_n,
            x.res,
            x._ref
          )
        })

    dimensions.show(1000)

    spark.stop()
  }

}

final case class Dimensions(
                             date:Date,
                             appId:Long,
                             action_name:String,
                             e_a:String,
                             e_c:String,
                             e_n:String,
                             res:String,
                             _ref:String,
                             device:String,
                             os:String,
                             osVersion:String
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

