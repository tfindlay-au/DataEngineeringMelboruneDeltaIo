import org.apache.spark.sql.SparkSession

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.util.Random

object DataGen {

  /**
   * Data structure for our fictitious weather data
   * @param obs_timestamp Reference to a point in time that the observation pertains to
   * @param weekday Category based on the observation timestamp
   * @param min Lowest expected temperature value
   * @param max Highest expected temperature value
   */
  case class Observation (
                           obs_timestamp: Timestamp,
                           weekday: String,
                           min: Int,
                           max: Int
                         )

  val obs_date: Calendar = Calendar.getInstance

  /**
   * Clunky function to generate single observation for a timestamp
   * @param sdf Tool to format day of week for dates
   * @param random Tool to generate random numbers
   * @return Observation object
   */
  def makeObservation(sdf: SimpleDateFormat, random: Random): Observation = {
    obs_date.add(Calendar.HOUR_OF_DAY, 1)

    val point_in_time = new Timestamp(obs_date.getTimeInMillis)
    Observation(
      point_in_time,
      sdf.format(obs_date.get(Calendar.DAY_OF_WEEK)),
      random.nextInt(20),
      random.nextInt(40)
    )
  }

  /**
   * This program uses Apache Spark to generate a data frame and save it to disk using Delta-io
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // Configure a starting point for the dates
    obs_date.set(1970, 0, 1, 0, 0, 0)

    val random = new Random
    val sdf = new SimpleDateFormat("EE")

    // Instantiate Apache Spark using all local processors
    val spark = SparkSession.builder()
      .appName("Hello World")
      .master("local[*]")
      .getOrCreate()

    // Configure less logging for Spark to clean up the console output
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Generate a set of data 5M rows ~= 8Mb on disk
    val start_time = System.nanoTime()

    Seq.fill(5000000){makeObservation(sdf, random)}
      .toDS()
      .write
      .format("delta")
      .save(".\\data")

    // Compute some primitive performance metrics
    val stop_time = System.nanoTime()
    val seconds = (stop_time - start_time) / 1000000000
    println("Elapsed time: " + (stop_time - start_time) + " ns or "+ seconds + " seconds")
    println("Rate: " + (10000 / seconds) + " per second")

  }
}
