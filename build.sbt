lazy val commonSettings = Seq(
  organization := "com.hope",
  organizationName := "experiment.spark",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.12.7"
)

lazy val sparkCommon = Project(id = "sparkCommon", base = file("sparkCommon"))
  .settings(commonSettings)
  .settings(
    name := "sparkCommon",
    description := "common module for spark experiment project"
  )

//lazy val wikipedia = (project in file("wikipedia"))
//  .settings(commonSettings)
//  .settings(
//    name := "wikipedia",
//    description := "wikipedia data access"
//  ).dependsOn(sparkCommon)

lazy val miscSparkSamples = (project in file("miscSparkSamples"))
  .settings(commonSettings)
  .settings(
    name := "miscSparkSamples",
    description := "miscellaneous spark sample implementations "
  ).dependsOn(sparkCommon)