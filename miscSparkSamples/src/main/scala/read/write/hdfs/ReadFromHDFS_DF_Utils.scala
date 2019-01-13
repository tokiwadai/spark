package read.write.hdfs

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.types._

case class User(id: Int, email: String, lang: String, country: String)

case class Transaction(id: Int, prodId: Int, userId: Int, purchaseAmt: Int, item: String)

case class Purchase(id: Int, prodId: Int)


object Utils {
  val transSchema = StructType {
    StructField("transId", IntegerType, false) ::
      StructField("prodId", IntegerType, true) ::
      StructField("userId", IntegerType, false) ::
      StructField("purchaseAmt", IntegerType, true) ::
      StructField("item", StringType, true) :: Nil
  }

  val userSchema = StructType {
    StructField("id", IntegerType, true) ::
      StructField("email", StringType, true) ::
      StructField("lang", StringType, true) ::
      StructField("country", StringType, true) :: Nil
  }
}

class ProcessJob_SparkSQL(session: SparkSession) {
  import session.implicits._

  def getNumberOfPurchasesPerUser_DS1(t: Dataset[Transaction], u: Dataset[User])  = {
    t.join(u, 'userId === u("id"), "left_outer")
      .select(u("id"), 'prodId).distinct
      .groupBy(u("id"))
      .agg(count('prodId))
      .sort(u("id"))
  }

  def getNumberOfPurchasesPerUser_DS(t: Dataset[Transaction], u: Dataset[User])  = {
    t.join(u, 'userId === u("id"), "left_outer")
      .select(u("id"), 'prodId).distinct.as[Purchase]
      .groupByKey(l => l.id)
      .agg(count('prodId).as[Int])
      .as[Purchase]
  }

  def getNumberOfCountriesPerProduct_DS(t: Dataset[Transaction], u: Dataset[User]): DataFrame  = {
    t.join(u, 'userId === u("id"), "left_outer")
      .select('prodId, 'country).distinct
      .groupBy('prodId)
      .agg(count('country))
      .sort('prodId)
  }

  def getNumberOfPurchasesPerUser_DF(t: DataFrame, u: DataFrame)  = {
    t.join(u, t("userId") === u("id"), "left_outer")
      .select(u("id"), t("prodId")).distinct
      .groupBy(u("id"))
      .agg(count(t("prodId")))
      .sort(u("id"))
  }

  def getNumberOfCountriesPerProduct_DF(t: DataFrame, u: DataFrame)  = {
    t.join(u, t("userId") === u("id"), "left_outer")
      .select(t("prodId"), u("country")).distinct
      .groupBy(t("prodId"))
      .agg(count(u("country")))
      .sort(t("prodId"))
  }
}