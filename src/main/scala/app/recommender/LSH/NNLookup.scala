package app.recommender.LSH

import org.apache.spark.rdd.RDD

/**
 * Class for performing LSH lookups
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookup(lshIndex: LSHIndex) extends Serializable {

  /**
   * Lookup operation for queries
   *
   * @param queries The RDD of keyword lists
   * @return The RDD of (keyword list, result) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {
    lshIndex.lookup(
      // Perform the hash on the queries to have for each list of genres its hash value
      lshIndex.hash(queries)
    )
      // Map the list of genres and the list of movies
      .map(x => (x._2, x._3))
  }
}
