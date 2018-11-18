package read.write.hdfs

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object ReadFromHDFS_DF extends Info {
  val trans_path = trans_FileCsv
  val users_path = users_FileCsv

  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder
      .master(master)
      .appName("ReadFromHDFS_DF")
      .getOrCreate()

    val reader = new ReadFromHDFS_DFschema(sparkSession)
    val tDF: DataFrame = reader.getTransactionsDS(trans_path)
    tDF.printSchema
    tDF.show(20)

    val uDF = reader.getUsersDS(users_path)
    uDF.printSchema
    uDF.show(20)

    val job = new ProcessJob_SparkSQL(sparkSession)
    val purchasesPerUser = job.getNumberOfPurchasesPerUser_DF(tDF, uDF)
    purchasesPerUser.show(20)

    val countriesPerProduct = job.getNumberOfCountriesPerProduct_DF(tDF, uDF)
    countriesPerProduct.show(20)

    //val output = args(2)
    //results.saveAsTextFile(output)
    sparkSession.stop()
  }
}
