package com.zhimo.datahub.etl.stage1

import com.zhimo.datahub.common.{ConfigHelper, SparkSessionSingleton}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
  * Created by yaning on 6/13/18.
  */
object MKSyncBusinessRelations  extends Logging{

  val config = new ConfigHelper(this)
  val processFromStart = config.getBoolean("processFromStart")
  val localDevEnv = config.getBoolean("localDev")

  // Template: Specify storage Parquet file
  val courseStorage = config.getEnvironmentString("storage.relations.course")
  val classStorage = config.getEnvironmentString("storage.relations.class")
  val gradeStorage = config.getEnvironmentString("storage.relations.grade")
  val subjectStorage = config.getEnvironmentString("storage.relations.subject")
  val teacherStorage = config.getEnvironmentString("storage.relations.teacher")


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

  //TODO: get all relations from MK live slave server
  //TODO: store relations to hive


}
