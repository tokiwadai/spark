val props = settingKey[Map[String, String]]("props")
props := {
  val log = sLog.value
  import scala.collection.JavaConverters._
  val prop = new java.util.Properties()
  val propertyFile = baseDirectory.value / ".."/"project" / "build.properties"
  log.info(s"loading properties file [$propertyFile]")
  IO.load(prop, propertyFile)
  val map = prop.stringPropertyNames.asScala.map(pr => pr -> Option(System.getProperty(pr)).getOrElse(prop.getProperty(pr))).toMap
  log.info(s"map: ${map.mkString(", ")}")
  map
}

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % props.value("akka.version"),
  "com.typesafe.akka" %% "akka-slf4j" % props.value("akka.version"),
  "org.apache.spark" %% "spark-core" % props.value("spark.version"),
  "org.apache.spark" %% "spark-sql" % props.value("spark.version"),
  "org.apache.spark" %% "spark-mllib" % props.value("spark.version"),
  "org.apache.spark" %% "spark-streaming" % props.value("spark.version"),
  "org.apache.spark" %% "spark-hive" % props.value("spark.version"),

  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.akka" %% "akka-testkit" % props.value("akka.version") % Test,
  "org.scalatest" %% "scalatest" % props.value("scalatest.version") ,
  "org.scalacheck" %% "scalacheck" % props.value("scalacheck.version"),
  "junit" % "junit" % props.value("junit.version")
)