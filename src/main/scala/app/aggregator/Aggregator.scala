package app.aggregator

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  // RDD to store the rating sum and count by movie_id
  private var averageRatingByMovie: RDD[(Int, (String, (Double, Int), List[String]))] = _

  // Partitioner used to distribute the RDD averageRatingByMovie by the movie_id attribute
  private var partitioner: HashPartitioner = _

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *                format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {
    // Compute the rating sum and count for each title
    val avgRatingsRDD: RDD[(Int, (Double, Int))] = ratings
      // Extract movie_id and rating
      .map(x => (x._2, x._4))
      // Group by movie_id
      .groupByKey()
      // Compute rating sum and size
      .mapValues(ratings => (ratings.sum, ratings.size))

    // Perform a left outer join with the movies RDD
    averageRatingByMovie = title.map(x => (x._1, (x._2, x._3)))
      .leftOuterJoin(avgRatingsRDD)
      .map { case (movieId, ((title, keywords), rating)) =>
        // If the join returns None as Option then set the sum and size to 0
        val avgRating: (Double, Int) = rating.getOrElse((0.0d, 0))
        (movieId, (title, avgRating, keywords))
      }

    // Partition the result
    partitioner = new HashPartitioner(averageRatingByMovie.partitions.length)
    averageRatingByMovie = averageRatingByMovie.partitionBy(partitioner)

    // Persist the resulting RDD
    averageRatingByMovie.persist()
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] =
    averageRatingByMovie
      // Reconstruct the average by dividing the sum by the count
      .map(x => (x._2._1, if (x._2._2._2 != 0) x._2._2._1 / x._2._2._2.toDouble else 0.0d))


  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {
    // Filter the movies that contain ALL the keywords
    val filteredTitles: RDD[(Int, (String, (Double, Int), List[String]))] = averageRatingByMovie
      .filter(x => keywords.forall(keyword => x._2._3.toSet.contains(keyword)))

    val titlesCount: Long = filteredTitles.count()

    if (titlesCount == 0) -1.0d // If no title matching those keywords has been found return -1
    else // Compute the average by dividing the sum by the occurrences and then average them all
      filteredTitles
        // Filter out the ones with sum 0
        .filter(_._2._2._1 != 0)
        .map(x => x._2._2._1 / x._2._2._2.toDouble)
        .mean()
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   * @param delta Delta ratings that haven't been included previously in aggregates
   *              format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val newRatingsRDD: RDD[(Int, (Double, Int))] = {
      // Transform delta into an RDD
      sc.parallelize(delta_)
        .map(x => {
          // If there's no previous rating, then count it as one rating
          if (x._3.isEmpty) {
            (x._2, (x._4, 1))
          } else {
            // If there's already a previous rating then don't increment the counter
            // and make it counting as the difference of the two values
            (x._2, (x._4 - x._3.get, 0))
          }
        })
        // Reduce by movie_id (sum the totals and the counters)
        .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    }

    val newAggregatedRDD: RDD[(Int, (String, (Double, Int), List[String]))] =
      averageRatingByMovie
        .leftOuterJoin(newRatingsRDD)
        .mapValues(x => {
          if (x._2.isEmpty) { // If the movies isn't present in the new ratings, simply keep the old one
            x._1
          } else { // Else sum the new values to the old ones
            val newRating: (Double, Int) = (x._1._2._1 + x._2.get._1, x._1._2._2 + x._2.get._2)
            (x._1._1, newRating, x._1._3)
          }
        })

    // Un-persist the old RDD
    averageRatingByMovie.unpersist()
    // Partition and persist the new updated RDD
    averageRatingByMovie = newAggregatedRDD.partitionBy(partitioner).persist()
  }
}
