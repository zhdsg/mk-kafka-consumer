package com.zhimo.datahub.common

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY

object Geo2IPHelper {

  @transient var ids:Array[GeoData] = _
  @transient var ranges:Array[GeoDataRange] = _

  def init(localDevEnv: Boolean, spark: SparkSession, config: ConfigHelper,forceOverwrite:Boolean = false): Unit = {

    import spark.implicits._
    var geolocation_ids: Dataset[GeoData] = null
    var geolocation_ranges: Dataset[GeoDataRange] = null
    if (forceOverwrite || config.getBoolean("location.overwrite") || (!PersistenceHelper.exists(localDevEnv, spark, config.getString("location.ids.table")))) {
      geolocation_ids = spark.read.option("header", "true").csv(config.getString("location.ids.source")).withColumn("locId", $"locId".cast("Long")).as[GeoData].persist(MEMORY_ONLY)
      PersistenceHelper.save(localEnvironment = localDevEnv, dataFrame = geolocation_ids.toDF(), table = config.getString("location.ids.table"), partitionBy = null, overwrite = true)
    } else {
      geolocation_ids = PersistenceHelper.load(localDevEnv, spark, config.getString("location.ids.table")).as[GeoData].persist(MEMORY_ONLY)
    }
    if (forceOverwrite || config.getBoolean("location.overwrite") || (!PersistenceHelper.exists(localDevEnv, spark, config.getString("location.ranges.table")))) {
      geolocation_ranges = spark.read.option("header", "true").csv(config.getString("location.ranges.source")).withColumn("startIpNum", $"startIpNum".cast("Long")).withColumn("endIpNum", $"endIpNum".cast("Long")).withColumn("locId", $"locId".cast("Long")).as[GeoDataRange].persist(MEMORY_ONLY)
      PersistenceHelper.save(localEnvironment = localDevEnv, dataFrame = geolocation_ranges.toDF(), table = config.getString("location.ranges.table"), partitionBy = null, overwrite = true)
    } else {
      geolocation_ranges = PersistenceHelper.load(localDevEnv, spark, config.getString("location.ranges.table")).as[GeoDataRange].persist(MEMORY_ONLY)
    }
    ids = geolocation_ids.collect()
    ranges = geolocation_ranges.collect()
  }

  def getLocation(locIP: Long): String = {
    var idxLow = 0
    var idxHigh = ranges.length
    var idx = 0
    var locId:Long = 0
    var protection = ranges.length

    while((idxLow<idxHigh) && (protection>0)){
      idx = idxLow + (idxHigh - idxLow) / 2
      if ((locIP >= ranges(idx).startIpNum) && (locIP <= ranges(idx).endIpNum)) {
        locId = ranges(idx).locId
      } else if (locIP > ranges(idx).startIpNum) {
        idxLow = idx
      } else if (locIP < ranges(idx).startIpNum) {
        idxHigh = idx
      }
      protection-=1
    }
    if(locId==0){
      "unknown"
    }else{
      idxLow = 0
      idxHigh = ids.length
      protection = ids.length
      var geo:GeoData = null

      while((idxLow<idxHigh) && (protection>0)) {
        idx = idxLow + (idxHigh - idxLow) / 2
        if (locId == ids(idx).locId) {
          geo = ids(idx)
        } else if (locId > ids(idx).locId) {
          idxLow = idx
        } else if (locId < ids(idx).locId) {
          idxHigh = idx
        }
        protection-=1
      }
      if(geo==null){
        "unknown"
      }else{
        geo.country+"-"+geo.city
      }
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
                          locId: Long,
                          country: String,
                          city: String
                        )

final case class GeoDataRange(
                               startIpNum: Long,
                               endIpNum: Long,
                               locId: Long
                             )