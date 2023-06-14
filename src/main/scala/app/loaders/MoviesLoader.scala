package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {

    /* movies.csv row: id|name|keyword_1|keyword_2| . . . |keyword_k */

    val moviesRDD: RDD[(Int, String, List[String])] =
      sc.textFile("src/main/resources/" + path)
        // Remove the quotes (") and split the string
        .map(_.replace("\"", "").split('|'))
        .map(x => (x(0).toInt, x(1), x.drop(2).toList))

    // Persist the RDD in memory
    moviesRDD.persist()
  }
}

