package com.zhimo.datahub.etl.stage1

import com.zhimo.datahub.common.{ConfigHelper, PersistenceHelper, SparkSessionSingleton}
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
  val courseTableName = config.getEnvironmentString("mysql.course_table")
  val classTableName = config.getEnvironmentString("mysql.class_table")
  val gradeTableName = config.getEnvironmentString("mysql.grade_table")
  val subjectTableName = config.getEnvironmentString("mysql.subject_table")
  val teacherTableName = config.getEnvironmentString("mysql.teacher_table")

  val courseStorage = config.getEnvironmentString("storage.relations.course")
  val classStorage = config.getEnvironmentString("storage.relations.class")
  val gradeStorage= config.getEnvironmentString("storage.relations.grade")
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
  val courseRelationTable = PersistenceHelper.loadFromMysql(localDevEnv,spark,courseTableName)
  val classRelationTable = PersistenceHelper.loadFromMysql(localDevEnv,spark,classTableName)
  val gradeRelationTable = PersistenceHelper.loadFromMysql(localDevEnv,spark,gradeTableName)
  val subjectRelationTable = PersistenceHelper.loadFromMysql(localDevEnv,spark,subjectTableName)
  val teacherRelationTable = PersistenceHelper.loadFromMysql(localDevEnv,spark,teacherTableName)

  courseRelationTable.show(10)
  classRelationTable.show(10)
  gradeRelationTable.show(10)
  subjectRelationTable.show(10)
  teacherRelationTable.show(10)
  //TODO: store relations to hive

  PersistenceHelper.save(localDevEnv, courseRelationTable, config.getEnvironmentString("storage.relations.course_relation"), null, true)
  PersistenceHelper.save(localDevEnv, classRelationTable, config.getEnvironmentString("storage.relations.class_relation"), null, true)
  PersistenceHelper.save(localDevEnv, gradeRelationTable, config.getEnvironmentString("storage.relations.grade_relation"), null, true)
  PersistenceHelper.save(localDevEnv, subjectRelationTable, config.getEnvironmentString("storage.relations.subject_relation"), null, true)
  PersistenceHelper.save(localDevEnv, teacherRelationTable, config.getEnvironmentString("storage.relations.teacher_relation"), null, true)
}
