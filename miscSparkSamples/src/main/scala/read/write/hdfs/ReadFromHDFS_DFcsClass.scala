package read.write.hdfs

import org.apache.spark.sql.{DataFrame, SparkSession}


class ReadFromHDFS_DFcsClass(session: SparkSession) {

  import session.implicits._

  def getTransactions(path: String, header: Boolean = true) = {
    if (header)
      convert2DS(path, header).as[Transaction]
    else
      convert2DS(path, header)
        .select(
          '_c0.cast("integer").as('id),
          '_c1.cast("integer").as('prodId),
          '_c2.cast("integer").as('userId),
          '_c3.cast("integer").as('purchaseAmt),
          '_c4.as('item))
        .as[Transaction]
  }

  def getUsers(path: String, header: Boolean = true) = {
    if (header)
      convert2DS(path, header).as[User]
    else
      convert2DS(path, header)
        .select(
          $"_c0".cast("integer").as('id),
          $"_c1".as('email),
          $"_c2".as('lang),
          $"_c3".as('country))
        .as[User]
  }

  def convert2DS(path: String, header: Boolean = true) = {
    if (header)
      session.read
        .option("header", "true") //reading the headers
        .option("inferSchema", true)
        .csv(path)
    else
      session.read
        .option("header", "false")
        .option("inferSchema", false)
        .csv(path)
  }
}
