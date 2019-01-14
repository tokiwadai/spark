package read.write.hdfs

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

trait Info {
  val trans_HDFSpath = "hdfs://hadoop-master:54311//user/tokiwadai/input/transactions.csv"
  val trans_FileCsv = getClass.getResource("/input/transactions.csv").getFile
  val trans_FileCsvNoHdr = getClass.getResource("/input/transactions_NoHdr.csv").getFile
  val trans_FileTsv = getClass.getResource("/input/transactions.txt").getFile


  val users_HDFSpath = "hdfs://hadoop-master:54311//user/tokiwadai/input/users.csv"
  val users_FileCsv = getClass.getResource("/input/users.csv").getFile
  val users_FileCsvNoHdr = getClass.getResource("/input/users_NoHdr.csv").getFile
  val users_FileTsv =  getClass.getResource("/input/users.txt").getFile

  val output = getClass.getResource("/output").getFile

  val local = "local[4]"
  val master = "spark://hadoop-master:7077"
  val server = master

  val uid = "UID"
  val typ = "type"
  val country = "country"

  val countryOfTaxation = List(
    ("1", "country", "AA"),
    ("2", "country", "BB"),
    ("3", "country", "CC"),
    ("4", "country", "DD")
  )

  val countryOfDomicile = List(
    ("1", "country", "AA"),
    ("2", "country", "BB"),
    ("5", "country", "EE"),
    ("6", "country", "FF")
  )

  val countryOfIncorporation = List(
    ("1", "country", "AA"),
    ("3", "country", "CC"),
    ("5", "country", "EE"),
    ("7", "country", "GG")
  )

  def outerJoinDF(dfLeft: DataFrame, dfRight: DataFrame, list: Array[Column]): DataFrame =
    outerJoinDF((dfLeft, uid), (dfRight, uid), list)

  def outerJoinDF(left: (DataFrame, String), right: (DataFrame, String), list: Array[Column]): DataFrame = {
    val (dfLeft, uidLeft) = left
    val uidL = s"${uidLeft}_Left"
    val typL = s"${typ}_Left"
    val dfLeftRenamed = dfLeft
      .withColumnRenamed(uidLeft, uidL)
      .withColumnRenamed(typ, typL)
    dfLeftRenamed.printSchema

    val (dfRight, uidRight) = right
    val uidR = s"${uidRight}_Right"
    val typR = s"${typ}_Right"
    val dfRightRenamed = dfRight
      .withColumnRenamed(uidRight, uidR)
      .withColumnRenamed(typ, typR)
    dfRightRenamed.printSchema

    val dfRslt = dfLeftRenamed
      .join(dfRightRenamed, col(uidL) === col(uidR), "outer")
      .withColumn(uid, when(col(uidL).isNull, col(uidR)).otherwise(col(uidL)))
      .withColumn(typ, when(col(typL).isNull, col(typR)).otherwise(col(typL)))
      .select(Array(col(uid), col(typ)) ++ list: _*)
    dfRslt.printSchema
    dfRslt
  }
}
