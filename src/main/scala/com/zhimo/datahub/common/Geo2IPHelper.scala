package com.zhimo.datahub.common

import org.apache.spark.sql.{ Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY

class Geo2IPHelper(localDevEnv: Boolean, spark: SparkSession, config: ConfigHelper) {

  private var geolocation: Dataset[GeoData] = null

  private def init(): Unit = {

    import spark.implicits._

    if (config.getBoolean("location.overwrite") || (!PersistenceHelper.exists(localDevEnv, spark, config.getString("location.table")))) {
      geolocation = spark.read.option("header", "true").csv(config.getString("location.source")).as[GeoData].persist(MEMORY_ONLY)
      PersistenceHelper.save(localDevEnv, geolocation.toDF(), config.getString("location.table"), null, true)
    } else {
      geolocation = PersistenceHelper.load(localDevEnv, spark, config.getString("location.table")).as[GeoData]
    }
  }

  init()

  def getLocation:Dataset[GeoData] = {
    geolocation
  }

}

object Geo2IPHelper {

  def ip2LocId(ip: String): Long = {
    val validNum = """(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9][0-9]|[0-9])"""
    val dot = """\."""
    val validIP = (validNum + dot + validNum + dot + validNum + dot + validNum).r

    try {
      ip match {
        case validIP(_, _, _, _) => {
          val bytes = ip.split('.').map(_.toInt.toLong)
          16777216L * bytes(0)+(65536L * bytes(1))+(256L * bytes(2))+bytes(3)
        }
      }
    } catch {
      case _: Throwable => 0
    }
  }

}

final case class GeoData(
                              locId:String,
                              city:String
                              )