package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load(): RDD[(Int, Int, Option[Double], Double, Int)] = {

    /* ratings.csv row: id_user|id_movie|rating|timestamp */

    val ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)] =
      sc.textFile("src/main/resources/" + path)
        .map(_.split('|')).map(line =>
        (line(0).toInt, line(1).toInt, None, line(2).toDouble, line(3).toInt))

    // Persist the RDD in memory
    ratingsRDD.persist()
  }
}