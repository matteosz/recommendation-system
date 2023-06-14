package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime


class SimpleAnalytics() extends Serializable {

  // RDD to store titles by movie_id
  private var titlesGroupedById: RDD[(Int, Iterable[(Int, String, List[String])])] = _
  // RDD to store rating tuples by year and then by movie_id
  private var ratingsGroupedByYearByTitle: RDD[((Int, Int),
    Iterable[(Int, Int, Option[Double], Double, Int)])] = _

  // Partitioners used for the two RDDs above
  private var ratingsPartitioner: HashPartitioner = _
  private var moviesPartitioner: HashPartitioner = _

  // Intermediate results reused
  private var movieIdMostRatedByYear: RDD[(Int, Int)] = _
  private var genresByYear: RDD[(Int, List[String])] = _

  private var ratings: RDD[(Int, Int, Option[Double], Double, Int)] = _
  private var movie: RDD[(Int, String, List[String])] = _

  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {

    this.ratings = ratings
    this.movie = movie

    // Pre-process the movies RDD grouping by movie_id
    titlesGroupedById = movie.groupBy(_._1)

    // Pre-process the ratings RDD grouping by year (need to convert the timestamp as it's in Unix epoch)
    // and then grouping each group by movie id
    ratingsGroupedByYearByTitle = ratings.map(x => (x._1, x._2, x._3, x._4,
      new DateTime(x._5 * 1000L).year().get()))
      .groupBy(x => (x._5, x._2))

    // Pre-process to compute the most rated movie by year
    movieIdMostRatedByYear = ratingsGroupedByYearByTitle.map(x => (x._1, x._2.size))
      .map { case ((year, movie), count) => (year, (movie, count)) }
      // Find the maximum ratings count by movie by year
      .reduceByKey((a, b) => {
        var movieId: Int = a._1
        val count: Int = math.max(a._2, b._2)
        if (b._2 > a._2) { // If b has greater count that take the b's title
          movieId = b._1
        } else if (b._2 == a._2) { // If count is equal take the greater id's value
          movieId = math.max(a._1, b._1)
        }

        (movieId, count)
      })
      // Map to (movie_id, year) to prepare for the join
      .map(x => (x._2._1, x._1))

    // Save (year, list(genres))
    genresByYear = movieIdMostRatedByYear.join(
      movie.map(x => (x._1, x._3))
    ).map(x => (x._2._1, x._2._2))

    // Partition the results
    moviesPartitioner = new HashPartitioner(titlesGroupedById.partitions.length)
    ratingsPartitioner = new HashPartitioner(ratingsGroupedByYearByTitle.partitions.length)
    titlesGroupedById = titlesGroupedById.partitionBy(moviesPartitioner)
    ratingsGroupedByYearByTitle = ratingsGroupedByYearByTitle.partitionBy(ratingsPartitioner)
    movieIdMostRatedByYear = movieIdMostRatedByYear.partitionBy(
      new HashPartitioner(movieIdMostRatedByYear.partitions.length)
    )
    genresByYear = genresByYear.partitionBy(
      new HashPartitioner(genresByYear.partitions.length)
    )

    // Persist the results
    titlesGroupedById.persist()
    ratingsGroupedByYearByTitle.persist()
    movieIdMostRatedByYear.persist()
    genresByYear.persist()
  }

  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = {
    // Simply map 1 for each movie in a specific year and then sum by the key
    ratingsGroupedByYearByTitle
      .map { case ((year, _), _) => (year, 1) }
      .reduceByKey(_ + _)
  }

  def getMostRatedMovieEachYear: RDD[(Int, String)] =
  // Return RDD (year, title)
    movieIdMostRatedByYear.join(
      movie.map(x => (x._1, x._2))
    )
      .map(x => (x._2._1, x._2._2))

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = genresByYear


  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = {
    // Find the ratings by genre (map to 1 and reduce by key)
    val genreOccurrences: RDD[(String, Int)] = genresByYear
      .flatMap {
        case (_, genres) =>
          genres.map(genre => (genre, 1))
      }.reduceByKey(_ + _)

    // Define a custom comparator to compare strings when counters are the same
    val comparator: Ordering[(String, Int)] = new Ordering[(String, Int)] {
      override def compare(x: (String, Int), y: (String, Int)): Int = {
        if (x._2 == y._2) {
          Ordering.String.compare(x._1, y._1) // Compare by genre name if occurrences are the same
        } else {
          Ordering.Int.compare(x._2, y._2) // Compare by occurrences
        }
      }
    }

    (genreOccurrences.min()(comparator), genreOccurrences.max()(comparator))
  }

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = {
    // Transform the requiredGenres into a set for efficient lookups
    val genresArray = requiredGenres.collect().toSet
    // Filter movies RDD based on requiredGenres
    val filteredMovies = movies.filter(movie => movie._3.exists(genre => genresArray.contains(genre)))

    // Return the RDD of movie titles
    filteredMovies.map(_._2)
  }

  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = {
    // Broadcast the requiredGenres list to all Spark executors instead of collecting
    val broadcastGenres = broadcastCallback(requiredGenres)
    // Filter movies RDD based on requiredGenres
    val filteredMovies = movies.filter(movie => movie._3.exists(genre => broadcastGenres.value.contains(genre)))

    // Return the RDD of movie titles
    filteredMovies.map(_._2)
  }

}

