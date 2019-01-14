package read.write.hdfs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OuterJoin_DF extends Info with Logging {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val cntryTax = s"${country}OfTaxation"
  val cntryDom = s"${country}OfDomicile"
  val domTaxCols = Array(col(cntryTax), col(cntryDom))

  val cntryInc = s"${country}OfIncorporation"
  val allCntryCols = domTaxCols :+ col(cntryInc)

  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder
      .appName("ReadFromHDFS_DS")
      .master(local)
      .getOrCreate()
    println("leftInnerJoin...")
    leftInnerJoin(sparkSession)

    sparkSession.stop
  }

  def leftInnerJoin(session: SparkSession) = {
    val sc = session.sparkContext
    // let df1 and df2 the Dataframes to merge
    val dfTax = session
      .createDataFrame(countryOfTaxation)
      .toDF(uid, typ, cntryTax)
      .withColumnRenamed(uid, "uidTax")
      .withColumnRenamed(typ, "typeTax")

    val dfDom = session
      .createDataFrame(countryOfDomicile)
      .toDF(uid, typ, cntryDom)
      .withColumnRenamed(uid, "uidDom")
      .withColumnRenamed(typ, "typeDom")

    val dfTaxDom = dfTax
      .join(dfDom, col("uidTax") === dfDom.col("uidDom"), "left")
      .where(col("uidDom").isNotNull)

    println("dfTaxDom...")
    dfTaxDom.show

    val dfTaxDom2 = dfTax
      .join(dfDom, col("uidTax") === col("uidDom"), "left")
      .where(col("uidDom").isNull)

    println("dfTaxDom2...")
    dfTaxDom2.show
  }
}
