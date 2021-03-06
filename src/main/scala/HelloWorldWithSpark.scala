import org.apache.spark.sql.SparkSession

object HelloWorldWithSpark {
  def main(args: Array[String]): Unit = {

    // Instantiate Apache Spark using all local processors
    val spark = SparkSession.builder()
      .appName("Hello World")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val data = Seq(
      ("Thursday", 12, 21),
      ("Friday", 14, 18),
      ("Saturday", 12, 21),
      ("Sunday", 11, 29),
      ("Monday", 16, 23),
      ("Tuesday", 15, 20),
      ("Wednesday", 13, 21)
    ).toDF("weekday", "min", "max")

    data.show()

  }

}
