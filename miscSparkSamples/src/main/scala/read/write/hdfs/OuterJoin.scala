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
    println("outerJoin...")
    outerJoin(sparkSession)

    println("outerJoin2...")
    outerJoin2(sparkSession)

    sparkSession.stop
  }

  def outerJoin(session: SparkSession) = {
    val sc = session.sparkContext
    // let df1 and df2 the Dataframes to merge
    val dfTax = session
      .createDataFrame(countryOfTaxation)
      .toDF(uid, typ, cntryTax)

    val dfDom = session
      .createDataFrame(countryOfDomicile)
      .toDF(uid, typ, s"${country}OfDomicile")

    val dfTaxDom = outerJoinDF(dfTax, dfDom, domTaxCols)
      .sort(col(uid).asc)
    println("dfTaxDom...")
    dfTaxDom.show

    val dfInc = session
      .createDataFrame(countryOfIncorporation)
      .toDF(uid, typ, cntryInc)

    val dfAllCntry = outerJoinDF(dfTaxDom, dfInc, allCntryCols)
      .sort(col(uid).asc)
    println("dfAllCntry...")
    dfAllCntry.show
  }

  def outerJoin2(session: SparkSession) = {
    val sc = session.sparkContext
    // let df1 and df2 the Dataframes to merge
    val uidT = s"${uid}_T"
    val dfTax = session
      .createDataFrame(countryOfTaxation)
      .toDF(uidT, typ, cntryTax)

    val uidD = s"${uid}_D"
    val dfDom = session
      .createDataFrame(countryOfDomicile)
      .toDF(uidD, typ, s"${country}OfDomicile")

    val dfTaxDom = outerJoinDF((dfTax, uidT), (dfDom, uidD), domTaxCols)
      .sort(col(uid).asc)
    println("dfTaxDom...")
    dfTaxDom.show

    val uidI = s"${uid}_I"
    val dfInc = session
      .createDataFrame(countryOfIncorporation)
      .toDF(uidI, typ, cntryInc)

    val dfAllCntry = outerJoinDF((dfTaxDom, uid), (dfInc, uidI), allCntryCols)
      .sort(col(uid).asc)
    println("dfAllCntry...")
    dfAllCntry.show
  }
}
