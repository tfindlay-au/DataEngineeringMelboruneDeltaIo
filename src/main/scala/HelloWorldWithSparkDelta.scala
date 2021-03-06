import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

object HelloWorldWithSparkDelta {
  def main(args: Array[String]): Unit = {

    //-----------------------------------------------------------------------
    // Configure the spark engine and Delta tooling

    val spark = SparkSession.builder()
      .appName("Hello World")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    // This simply reduced the "noise" in the console output
    spark.sparkContext.setLogLevel("ERROR")

    // This enables some nifty shortcuts like toDF()
    import spark.implicits._

    //-----------------------------------------------------------------------
    // Produce an initial sample set of data

    val data = Seq(
      ("Thursday", 12, 21),
      ("Friday", 14, 18),
      ("Saturday", 12, 21),
      ("Sunday", 11, 29),
      ("Monday", 16, 23),
      ("Tuesday", 15, 20),
    ).toDF("weekday", "min", "max")

    // Write the above dataframe to disk
    data.write.format("delta").save(".\\data")


    //-----------------------------------------------------------------------
    // Define an update/change set
    // 3 updates
    // 1 insert

    val new_data = Seq(
      ("Thursday", 12, 30),
      ("Friday", 14, 30),
      ("Saturday", 12, 31),
      ("Wednesday", 11, 42),
    ).toDF("weekday", "min", "max")

    DeltaTable.forPath(spark, ".\\data").alias("a")
      .merge(new_data.alias("b"),  "a.weekday = b.weekday")
      .whenMatched().updateAll()
      .whenNotMatched().insertAll()
      .execute()

    //-----------------------------------------------------------------------
    // Access / consuming the data:
    //    Lets read that data back in again and specify a particular version

    spark.read
      .format("delta")
      .option("versionAsOf", 0)
      .load(".\\data")
      .show()

    // Compare this to a "current" view, no filtering on high watermark values

    spark.read
      .format("delta")
      .load(".\\data")
      .show()

  }
}
