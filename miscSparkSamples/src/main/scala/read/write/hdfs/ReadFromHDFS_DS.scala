package read.write.hdfs

import java.io.File

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}


object ReadFromHDFS_DS extends Info with Logging {
  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder
      .appName("ReadFromHDFS_DS")
      .master(local)
      .getOrCreate()

    // Processing with Header
    //process(sparkSession, trans_FileCsv, users_FileCsv)

    // Processing with NO Header
    process(sparkSession, trans_FileCsvNoHdr, users_FileCsvNoHdr, false)

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
    val purchasesPerUser: Dataset[Row] = job.getNumberOfPurchasesPerUser_DS(tDS, uDS)
    purchasesPerUser.show(20)
    val outFile: String = new File("./ouput/purchasesPerUser").getAbsolutePath
    logInfo(s"printing to $outFile")
    purchasesPerUser.write.format("csv").option("header", "true").save(outFile)
    //purchasesPerUser.write.format("com.databricks.spark.csv").option("header", "true").save(outFile)
    //purchasesPerUser.rdd.map(x => x.mkString(",")).saveAsTextFile(outFile)

    val countriesPerProduct = job.getNumberOfCountriesPerProduct_DS(tDS, uDS)
    countriesPerProduct.show(20)
    val outFile2: String = new File("./ouput/countriesPerProduct").getAbsolutePath
    logInfo(s"printing to $outFile2")
    countriesPerProduct.write.format("csv").option("header", "true").save(outFile2)
    //countriesPerProduct.write.format("com.databricks.spark.csv").option("header", "true").save(outFile2)
    //countriesPerProduct.rdd.map(x => x.mkString(",")).saveAsTextFile(outFile2)
  }
}
