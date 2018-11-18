package read.write.hdfs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object ReadFromHFDS_RDD extends Info {
  val trans_path = trans_FileTsv
  val users_path = users_FileTsv

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("ReadFromHFDS_RDD")
      .setMaster(master)
    val ctx = new SparkContext(conf)
    val transactionsIn = ctx.textFile(trans_path)
    val usersIn = ctx.textFile(users_path)

    val job = new ProcessJob_RDD(ctx)
    val results = job.run(transactionsIn, usersIn)
    val output = getClass.getResource("/output").getFile
    results.saveAsTextFile(output)
    ctx.stop()
  }

  class ProcessJob_RDD(sc: SparkContext) {
    def run(transactions: RDD[String], users: RDD[String]) : RDD[(String, String)] = {
      val newTransactionsPair: RDD[(Int, Int)] = transactions.map{ t =>
        val p = t.split("\t")
        (p(2).toInt, p(1).toInt)
      }

      val newUsersPair: RDD[(Int, String)] = users.map{ t =>
        val p = t.split("\t")
        (p(0).toInt, p(3))
      }

      val result = processData(newTransactionsPair, newUsersPair)
      return sc.parallelize(result.toSeq).map(t => (t._1.toString, t._2.toString))
    }

    def processData (t: RDD[(Int, Int)], u: RDD[(Int, String)])  = {
      var jn = t.leftOuterJoin(u).values.distinct
      jn.countByKey
    }
  }
}


