package com.zhimo.datahub.common

import java.time.temporal.TemporalAmount
import java.time.{Duration, Period}
import java.util.concurrent.TimeUnit
import java.{lang, util}

import com.typesafe.config._

class ConfigHelper(source: AnyRef) {

  private val localConfig = ConfigFactory.load("application-local")
  private val config = if(localConfig==null) ConfigFactory.load() else localConfig

  private def path(s:String):String = {
    val specificPath = ConfigHelper.getClassName(source)+"."+s
    if(config.hasPath(specificPath)){
      specificPath
    }else{
      "common."+s
    }

  }

  def getEnvironmentString(s:String): String = {
    String.format(getString(s),getString("environment"))
  }

  def hasPath(s: String): Boolean = config.hasPath(path(s))

  def hasPathOrNull(s: String): Boolean = config.hasPathOrNull(path(s))

  def isEmpty: Boolean = config.isEmpty

  def getIsNull(s: String): Boolean = config.getIsNull(path(s))

  def getBoolean(s: String): Boolean = config.getBoolean(path(s))

  def getNumber(s: String): Number = config.getNumber(path(s))

  def getInt(s: String): Int = config.getInt(path(s))

  def getLong(s: String): Long = config.getLong(path(s))

  def getDouble(s: String): Double = config.getDouble(path(s))

  def getString(s: String): String = config.getString(path(s))

  def getEnum[T <: Enum[T]](aClass: Class[T], s: String): T = config.getEnum(aClass, path(s))

  def getObject(s: String): ConfigObject = config.getObject(path(s))

  def getConfig(s: String): Config = config.getConfig(path(s))

  def getAnyRef(s: String): AnyRef = config.getAnyRef(path(s))

  def getValue(s: String): ConfigValue = config.getValue(path(s))

  def getBytes(s: String): lang.Long = config.getBytes(path(s))

  def getMemorySize(s: String): ConfigMemorySize = config.getMemorySize(path(s))

  def getDuration(s: String, timeUnit: TimeUnit): Long = config.getDuration(path(s), timeUnit)

  def getDuration(s: String): Duration = config.getDuration(path(s))

  def getPeriod(s: String): Period = config.getPeriod(path(s))

  def getTemporal(s: String): TemporalAmount = config.getTemporal(path(s))

  def getList(s: String): ConfigList = config.getList(path(s))

  def getBooleanList(s: String): util.List[lang.Boolean] = config.getBooleanList(path(s))

  def getNumberList(s: String): util.List[Number] = config.getNumberList(path(s))

  def getIntList(s: String): util.List[Integer] = config.getIntList(path(s))

  def getLongList(s: String): util.List[lang.Long] = config.getLongList(path(s))

  def getDoubleList(s: String): util.List[lang.Double] = config.getDoubleList(path(s))

  def getStringList(s: String): util.List[String] = config.getStringList(path(s))

  def getEnumList[T <: Enum[T]](aClass: Class[T], s: String): util.List[T] = config.getEnumList(aClass,path(s))

  def getObjectList(s: String): util.List[_ <: ConfigObject] = config.getObjectList(path(s))

  def getConfigList(s: String): util.List[_ <: Config] = config.getConfigList(path(s))

  def getAnyRefList(s: String): util.List[_] = config.getAnyRefList(path(s))

  def getBytesList(s: String): util.List[lang.Long] = config.getBytesList(path(s))

  def getMemorySizeList(s: String): util.List[ConfigMemorySize] = config.getMemorySizeList(path(s))

  def getDurationList(s: String, timeUnit: TimeUnit): util.List[lang.Long] = config.getDurationList(path(s),timeUnit)

  def getDurationList(s: String): util.List[Duration] = config.getDurationList(path(s))

  def withOnlyPath(s: String): Config = config.withOnlyPath(path(s))

  def withoutPath(s: String): Config = config.withoutPath(path(s))

  def atPath(s: String): Config = config.atPath(path(s))

  def atKey(s: String): Config = config.atKey(path(s))

  def withValue(s: String, configValue: ConfigValue): Config = config.withValue(path(s),configValue)
}

object ConfigHelper{

  def getClassName(source:AnyRef): String ={
    val className = source.getClass.getSimpleName
    className.substring(0,className.length-1)
  }

}