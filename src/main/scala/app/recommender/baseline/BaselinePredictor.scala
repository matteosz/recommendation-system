package app.recommender.baseline

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

class BaselinePredictor() extends Serializable {

  // RDD to store the average rating for each user
  private var avgRatingByUserId: RDD[(Int, Double)] = _
  // RDD to store the global average deviation for each movie
  private var globalAvgDeviation: RDD[(Int, Double)] = _
  // RDD to store all the ratings to get the mean
  private var allRatings: RDD[Double] = _

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    // Find the average rating for each user
    avgRatingByUserId = ratingsRDD
      // Map (user_id, rating)
      .map(x => (x._1, x._4))
      // Group by user_id
      .groupByKey()
      .mapValues(x => x.sum / x.size.toDouble)

    // Find the global average rating (the mean of all ratings)
    allRatings = ratingsRDD.map(_._4)

    // Find the normalized deviation for each pair (user_id, movie_id)
    val normalizedDev: RDD[(Int, Int, Double)] = ratingsRDD
      .map(x => (x._1, (x._2, x._4)))
      // Join with average rating by user_id to have it in the tuple
      .join(avgRatingByUserId)
      // Map (user_id, movie_id, (rating - user_avg) / scale factor
      .map(x => (x._1, x._2._1._1, (x._2._1._2 - x._2._2) / scale(x._2._1._2, x._2._2)))

    globalAvgDeviation = normalizedDev
      // Group by movie_id
      .groupBy(_._2)
      .mapValues(x => {
        var sum: Double = 0.0d
        x.foreach(y => {
          // Sum each normalized deviation
          sum = sum + y._3
        })
        sum / x.size.toDouble
      })

    // Partition and persist the RDDs
    globalAvgDeviation = globalAvgDeviation.partitionBy(new HashPartitioner(globalAvgDeviation.partitions.length))
      .persist()
    avgRatingByUserId = avgRatingByUserId.partitionBy(new HashPartitioner(avgRatingByUserId.partitions.length))
      .persist()
    allRatings.persist()
  }

  def predict(userId: Int, movieId: Int): Double = {
    // Filter by user_id
    val avgRatingOfUserOpt: Option[(Int, Double)] = avgRatingByUserId
      .filter(_._1 == userId)
      .take(1)
      .headOption

    // Return global average if user has no rating
    if (avgRatingOfUserOpt.isEmpty) {
      return allRatings.mean()
    }

    val avgRatingOfUser: Double = avgRatingOfUserOpt.get._2

    // Filter by movie_id
    val globalAvgDevOpt: Option[(Int, Double)] = globalAvgDeviation
      .filter(_._1 == movieId)
      .take(1)
      .headOption

    // Return average rating if no rating for movie
    if (globalAvgDevOpt.isEmpty) {
      return avgRatingOfUser
    }

    val globalAvgDev: Double = globalAvgDevOpt.get._2

    // Use the formula
    avgRatingOfUser + globalAvgDev * scale(avgRatingOfUser + globalAvgDev, avgRatingOfUser)
  }

  /** Function used to normalize */
  private def scale(x: Double, r: Double): Double = if (x > r) 5.0d - r else if (x < r) r - 1.0d else 1.0d
}
