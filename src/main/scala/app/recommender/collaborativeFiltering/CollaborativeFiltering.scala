package app.recommender.collaborativeFiltering


import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD

class CollaborativeFiltering(rank: Int,
                             regularizationParameter: Double,
                             seed: Long,
                             n_parallel: Int) extends Serializable {

  private val maxIterations: Int = 20
  private var model: org.apache.spark.mllib.recommendation.MatrixFactorizationModel = _

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    // Convert into Rating objects
    val ratings = ratingsRDD.map(x => Rating(x._1, x._2, x._4))
    // Train the model
    model = ALS.train(ratings, rank, maxIterations, regularizationParameter, n_parallel, seed)
  }

  def predict(userId: Int, movieId: Int): Double = model.predict(userId, movieId)
}
