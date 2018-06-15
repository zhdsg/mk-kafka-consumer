package com.zhimo.datahub.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by yaning on 5/2/18.
  */
object SparkSessionSingleton {

  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf,hiveSupport: Boolean = true): SparkSession = {
    if (instance == null) {
      var builder = SparkSession
        .builder
        .config(sparkConf)
      if(hiveSupport) {
        builder = builder.enableHiveSupport()
      }
      instance = builder
        .getOrCreate()
    }
    instance
  }

  def getInstanceIfExists():SparkSession ={
    instance
  }

}
