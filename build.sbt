val localDev = true

name := "mk-kafka-consumer"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.2.0"

if(localDev){
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % "2.2.0",
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
    "org.apache.spark" %% "spark-hive" % sparkVersion,
    "org.uaparser" %% "uap-scala" % "0.2.0",
    "com.typesafe" % "config" % "1.3.3",
    "mysql" % "mysql-connector-java" % "5.1.43"
  )


}else {
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % "2.2.0",
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
    "org.apache.spark" %% "spark-hive" % sparkVersion,
    "org.uaparser" %% "uap-scala" % "0.2.0",
    "com.typesafe" % "config" % "1.3.3",
    "mysql" % "mysql-connector-java" % "5.1.43"

  )


}

assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}