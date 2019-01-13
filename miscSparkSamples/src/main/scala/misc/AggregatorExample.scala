package misc


import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

object AggregatorExample extends App {
  val spark = SparkSession.
    builder.master("local[*]")
    .appName("Simple Application")
    .getOrCreate()

  import spark.implicits._

  val list = List(
    (3, "Me"), (1, "Thi"), (2, "Se"),
    (3, "ssa"), (1, "s is a"), (3, "ge:"),
    (3, "-)"), (2, "cre"), (2, "t"))

  val ds = list.toDS

  val concat = new Aggregator[(Int, String), String, String] {
    override def zero: String = StringUtils.EMPTY

    override def reduce(b: String, a: (Int, String)): String = b + a._2

    override def merge(b1: String, b2: String): String = b1 + b2

    override def finish(reduction: String): String = reduction

    override def bufferEncoder: Encoder[String] = Encoders.STRING

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }.toColumn

  ds.groupByKey(item => item._1)
    .agg(concat.as[String])
    .sort('value)
    .show()

}
