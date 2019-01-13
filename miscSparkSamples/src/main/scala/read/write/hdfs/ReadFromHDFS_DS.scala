package read.write.hdfs

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction


object ReadFromHDFS_DS extends Info with Logging {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder
      .appName("ReadFromHDFS_DS")
      .master(local)
      .getOrCreate()
    does3(sparkSession)
    // Processing with Header
    //process(sparkSession, trans_FileCsv, users_FileCsv)

    // Processing with NO Header
    //process(sparkSession, trans_FileCsvNoHdr, users_FileCsvNoHdr, false)

    //val output = args(2)
    //results.saveAsTextFile(output)
    sparkSession.stop()
  }

  def process(sparkSession: SparkSession, tPath: String, uPath: String, header: Boolean = true) = {
    import sparkSession.implicits._

    val reader = new ReadFromHDFS_DFcsClass(sparkSession)
    val tDS = reader.getTransactions(tPath, header)
    tDS.printSchema
    tDS.show(20)

    val uDS = reader.getUsers(uPath, header)
    uDS.printSchema
    uDS.show(20)

    val job = new ProcessJob_SparkSQL(sparkSession)
    val purchasesPerUser = job.getNumberOfPurchasesPerUser_DS(tDS, uDS)
    purchasesPerUser.show(20)
    val outFile: String = new File("./ouput/purchasesPerUser").getAbsolutePath
    logInfo(s"printing to $outFile")
    //purchasesPerUser.write.format("csv").option("header", "true").save(outFile)
    //purchasesPerUser.write.format("com.databricks.spark.csv").option("header", "true").save(outFile)
    //purchasesPerUser.rdd.map(x => x.mkString(",")).saveAsTextFile(outFile)

    val countriesPerProduct = job.getNumberOfCountriesPerProduct_DS(tDS, uDS)
    countriesPerProduct.show(20)
    val outFile2: String = new File("./ouput/countriesPerProduct").getAbsolutePath
    logInfo(s"printing to $outFile2")
    //countriesPerProduct.write.format("csv").option("header", "true").save(outFile2)
    //countriesPerProduct.write.format("com.databricks.spark.csv").option("header", "true").save(outFile2)
    //countriesPerProduct.rdd.map(x => x.mkString(",")).saveAsTextFile(outFile2)
  }

  def does(session: SparkSession) = {
    import org.apache.spark.sql.functions._

    val sc = session.sparkContext
    // let df1 and df2 the Dataframes to merge
    val df1 = session.createDataFrame(List(
      ("50", 2),
      ("34", 4),
      ("32", 1)
    )).toDF("age", "children")

    val df2 = session.createDataFrame(List(
      ("26", "true", 60000.00),
      ("32", "false", 35000.00)
    )).toDF("age", "education", "income")

    val cols1: Set[String] = df1.columns.toSet
    val cols2: Set[String] = df2.columns.toSet
    val total: Set[String] = cols1 ++ cols2 // union

    def expr(myCols: Set[String], allCols: Set[String]) = {
      allCols.toList.map(x => x match {
        case x if myCols.contains(x) => col(x)
        case _ => lit("").as(x)
      })
    }

    val df2a = df1
      .join(df2, df1.col("age") === df2.col("age"))
      .select(df1.col("age"), col("children"), col("education"), col("income"))
    df2a.show

    val df3 = df1.select(expr(cols1, total):_*)
      .unionAll(df2.select(expr(cols2, total):_*))
    df3.show

    val df4 = df3
      .join(df2a, df3.col("age") !== df2a.col("age"))
      .select(df3.col("age"), df3.col("children"), df3.col("education"), df3.col("income"))
    df4.show

    df4.unionAll(df2a).show
  }

  def does2(session: SparkSession) = {
    import org.apache.spark.sql.functions._

    val sc = session.sparkContext
    // let df1 and df2 the Dataframes to merge
    val dfCI = session.createDataFrame(List(
      ("1", "ok", "AA"),
      ("2", "ok", "BB"),
      ("3", "ok", "CC"),
      ("4", "ok", "DD")
    )).toDF("uid", "status", "CI")

    val dfCD = session.createDataFrame(List(
      ("1", "ok", "AA"),
      ("2", "ok", "BB"),
      ("5", "ok", "EE"),
      ("6", "ok", "FF")
    )).toDF("uid", "status", "CD")

    val dfCID = dfCI
      .join(dfCD, dfCI.col("uid") === dfCD.col("uid"), "outer")
      .select(dfCI.col("uid").alias("uid_CI"), dfCD.col("uid").alias("uid_CD"),
        dfCI.col("status").alias("status_CI"), dfCD.col("status").alias("status_CD"),
        col("CI"), col("CD"))
      .withColumn("uid", when(col("uid_CI").isNull, col("uid_CD"))
        .otherwise(col("uid_CI")))
      .withColumn("status", when(col("status_CI").isNull, col("status_CD"))
        .otherwise(col("status_CI")))
      .select(col("uid"), col("status"),col("CI"), col("CD"))
      .sort(col("uid").asc)
    println("dfCID1...")
    dfCID.show

    val dfCT = session.createDataFrame(List(
      ("1", "ok", "AA"),
      ("3", "ok", "CC"),
      ("5", "ok", "EE"),
      ("7", "ok", "GG")
    )).toDF("uid", "status", "CT")

    val dfCIDb = dfCID
      .join(dfCT, dfCID.col("uid") === dfCT.col("uid"), "outer")
      .select(dfCID.col("uid").alias("uid_CID"), dfCT.col("uid").alias("uid_CT"),
        dfCID.col("status").alias("status_CID"), dfCT.col("status").alias("status_CT"),
        col("CI"), col("CD"), col("CT"))
      .withColumn("uid", when(col("uid_CID").isNull, col("uid_CT"))
        .otherwise(col("uid_CID")))
      .withColumn("status", when(col("status_CID").isNull, col("status_CT"))
        .otherwise(col("status_CID")))
      .select(col("uid"), col("status"),col("CI"), col("CD"), col("CT"))
      .sort(col("uid").asc)
    println("dfCID1b...")
    dfCIDb.show
  }

  def does3(session: SparkSession) = {
    import org.apache.spark.sql.functions._

    val sc = session.sparkContext
    // let df1 and df2 the Dataframes to merge
    val dfCI = session.createDataFrame(List(
      ("1", "ok", "AA"),
      ("2", "ok", "BB"),
      ("3", "ok", "CC"),
      ("4", "ok", "DD")
    )).toDF("uid", "status", "CI")

    val dfCD = session.createDataFrame(List(
      ("1", "ok", "AA"),
      ("2", "ok", "BB"),
      ("5", "ok", "EE"),
      ("6", "ok", "FF")
    )).toDF("uid", "status", "CD")

    val dfCT = session.createDataFrame(List(
      ("1", "ok", "AA"),
      ("3", "ok", "CC"),
      ("5", "ok", "EE"),
      ("7", "ok", "GG")
    )).toDF("uid", "status", "CT")


    val dfCID = dfCI
      .join(dfCD, dfCI.col("uid") === dfCD.col("uid"), "left_outer")
      .join(dfCT, dfCI.col("uid") === dfCT.col("uid"), "left_outer")
    println("left join -- dfCID1...")
    dfCID.show

    val dfCID2 = dfCI
      .join(dfCD, dfCI.col("uid") === dfCD.col("uid"))
      .select(dfCI.col("uid").alias("uid_CI"), dfCD.col("uid").alias("uid_CD"),
        dfCI.col("status").alias("status_CI"), dfCD.col("status").alias("status_CD"),
        col("CI"), col("CD"))
      .withColumn("uid", when(col("uid_CI").isNull, col("uid_CD"))
        .otherwise(col("uid_CI")))
      .withColumn("status", when(col("status_CI").isNull, col("status_CD"))
        .otherwise(col("status_CI")))
    println("inner join - dfCID2...")
    dfCID2.show
  }
}
