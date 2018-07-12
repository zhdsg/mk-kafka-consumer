package com.zhimo.datahub.common

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY
import org.apache.spark.broadcast.Broadcast

object Geo2IPHelper {


  def init(localDevEnv: Boolean, spark: SparkSession ,forceOverwrite:Boolean = false): Array[GeoData] = {
    var geo:Array[GeoData] = null
    val config = new ConfigHelper(this)

    import spark.implicits._
    var geolocation: Dataset[GeoData] = null

    if (forceOverwrite || config.getBoolean("location.overwrite") || (!PersistenceHelper.exists(localDevEnv, spark, config.getString("location.table")))) {
      geolocation = spark.read.option("header", "true").csv(config.getString("location.source")).withColumn("StartIPNum", $"StartIPNum".cast("Long")).withColumn("EndIPNum", $"EndIPNum".cast("Long")).as[GeoData].persist(MEMORY_ONLY)
      PersistenceHelper.save(localEnvironment = localDevEnv, dataFrame = geolocation.toDF(), table = config.getString("location.table"), partitionBy = null, overwrite = true)
    } else {
      if(localDevEnv) {
        geolocation = PersistenceHelper.load(localDevEnv, spark, config.getString("location.table")).withColumn("StartIPNum", $"StartIPNum".cast("Long")).withColumn("EndIPNum", $"EndIPNum".cast("Long"))
          .withColumnRenamed("Local", "local_city")
          .withColumnRenamed("Country", "country")
          .as[GeoData].persist(MEMORY_ONLY)
      }
      else {
        geolocation = PersistenceHelper.load(localDevEnv, spark, config.getString("location.table")).withColumn("StartIPNum", $"StartIPNum".cast("Long")).withColumn("EndIPNum", $"EndIPNum".cast("Long")).as[GeoData].persist(MEMORY_ONLY)
      }
    }


    geo = geolocation.collect()
    println("Geo2IPHelper initialized "+geo.length)
    geo
  }

  def getLocation(locIP: Long,geo:Broadcast[Array[GeoData]]): String = {
    var idxLow = 0
    var idxHigh = geo.value.length
    var idx = 0
    var gd: GeoData = null
    var protection = geo.value.length

    while ((idxLow < idxHigh) && (protection > 0)) {
      idx = idxLow + (idxHigh - idxLow) / 2
      if ((locIP >= geo.value(idx).StartIPNum) && (locIP <= geo.value(idx).EndIPNum)) {
        gd = geo.value(idx)
      } else if (locIP > geo.value(idx).StartIPNum) {
        idxLow = idx
      } else if (locIP < geo.value(idx).StartIPNum) {
        idxHigh = idx
      }
      protection -= 1
    }
    if (gd == null) {
      "unknown"
    } else {
      gd.country + " " + gd.local_city
    }
  }

  def ip2LocId(ip: String): Long = {
    val validNum = """(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9][0-9]|[0-9])"""
    val dot = """\."""
    val validIP = (validNum + dot + validNum + dot + validNum + dot + validNum).r

    try {
      ip match {
        case validIP(_, _, _, _) => {
          val bytes = ip.split('.').map(_.toInt.toLong)
          16777216L * bytes(0) + (65536L * bytes(1)) + (256L * bytes(2)) + bytes(3)
        }
      }
    } catch {
      case _: Throwable => 0
    }
  }

}

final case class GeoData(
                          StartIPNum: Long,
                          EndIPNum:Long,
                          country: String,
                          local_city: String
                        )