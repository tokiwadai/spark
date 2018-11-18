package read.write.hdfs

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
}
