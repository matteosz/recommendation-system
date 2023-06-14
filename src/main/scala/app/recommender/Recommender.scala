package app.recommender

import app.recommender.LSH.{LSHIndex, NNLookup}
import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for performing recommendations
 */
class Recommender(sc: SparkContext,
                  index: LSHIndex,
                  ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {

  // Initialize the predictors and create the LSH index
  private val nn_lookup = new NNLookup(index)
  private val collaborativePredictor = new CollaborativeFiltering(10, 0.1, 0, 4)
  collaborativePredictor.init(ratings)

  private val baselinePredictor = new BaselinePredictor()
  baselinePredictor.init(ratings)

  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = recommend(userId, genre, K, baselinePredictor.predict)

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = recommend(userId, genre, K, collaborativePredictor.predict)

  private def recommend(userId: Int, genre: List[String], K: Int, myPredictor: (Int, Int) => Double): List[(Int, Double)] = {
    // Find the movies already rated by the user
    val seenMovies: Set[Int] = ratings
      // Filter by user_id
      .filter(_._1 == userId)
      // Map the movie_id
      .map(_._2)
      // Use a set for fast lookups
      .collect().toSet

    val movies: List[Int] = sc.parallelize(
      // Retrieve the similar movies through a lookup
      nn_lookup.lookup(
        sc.parallelize(List(genre))
      )
        // Take the first list of movies only as the list of genres has only 1 element
        .map(_._2)
        .take(1)
        .head
    )
      // Filter out movies already rated by the user, no sense to predict on them
      .filter(x => !seenMovies.contains(x._1))
      // Take their ids
      .map(_._1)
      .collect().toList

    // Map applying the prediction function and sort in descending order by rating
    movies.map(x => (x, myPredictor(userId, x)))
      .sortBy(_._2)(Ordering.Double.reverse)
      // Take the K top most rated as prediction
      .take(K)
  }
}
