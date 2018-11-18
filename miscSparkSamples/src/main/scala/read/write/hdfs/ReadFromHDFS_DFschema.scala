package read.write.hdfs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType


class ReadFromHDFS_DFschema(session: SparkSession) {
  def getTransactionsDS(path: String) = {
    convert2DF(path, Utils.transSchema)
  }

  def getUsersDS(path: String) = {
    convert2DF(path, Utils.userSchema)
  }

  def convert2DF(path: String, schema: StructType) = {
    session.read
      .schema(schema)
      .csv(path)
  }
}
