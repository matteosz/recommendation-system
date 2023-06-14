# Project for the course Systems for Data Science and Data Management (CS-460)

Milestone 1
===========
Task 1
------

The `.csv` files are read through the method `sparkContext.textFile()`, which reads the file row by row and trasform them into an `RDD[String]`.
- MoviesLoader: the rows here are processed by removing the `"` and split to extract the movie's ID (`Int`), the movie's title (`String`) and its keyword (`List[String]`).
- RatingsLoader: with the ratings there's no usage of the `"` so each row is directly split and mapped to create an `RDD[(Int, Int, Option[Double], Double, Int)]`, where initially each option value is set to `None` as there's no any previous rating for a given user and movie.

Both RDDs are persisted in memory before returning.

Task 2.1
--------

The `init()` function is used to pre-process the dataset and it creates 4 RDDs, they're designed to serve for a future purpose in the `SimpleAnalytics` class, then each RDD is partitioned by its key and persisted in memory:

1. `titlesGroupedById`: it simply groups by movie ID all the titles. (`RDD[(Int, Iterable[(Int, String, List[String])])]`)

2. `ratingsGroupedByYearByTitle`: it first groups rating by the year and then, within an year, by movie ID. The timestamps are converted in year using the `DateTime` object and multiplying the timestamp by `1000` (as to convert in milliseconds). (`RDD[(Int, Int)]`)

3. `movieIdMostRatedByYear`: it is used to store for each year the movie ID of the movie most rated for that year. It is computed by using the year as key and calling `reduceByKey` on the `ratingsGroupedByYearByTitle`, such that at each iteration of the reduce the movie with highest count is kept (if counters are equal than the highest ID is chosen). (`RDD[(Int, Int)]`)

4. `genresByYear`: it stores the list of most rated genres by year. It's computed by joining together the `movieIdMostRatedByYear` and `movie` on the movie ID. (`RDD[(Int, List[String])]`)

Task 2.2
--------

To map for each year the number of rated movies we simply map `(year, 1)`, instead of keeping the whole information about the movie, for each entry in `ratingsGroupedByYearByTitle` and then we reduce by summing up all the ones by key.

Task 2.3
--------

To return the information about the most rated movie each year we perform a join operation on the `movieIdMostRatedByYear` and on the movies RDD, so that we can obtain the title information.

Task 2.4
--------

It just returns `genresByYear`.

Task 2.5
--------

We first map for each genre in `genresByYear` the tuple `(genre, 1)` and then we sum up all the 1s reducing by the genre key. This obtains for each genre the count of all the related ratings, so using the `max()` and `min()` function with the right comparator (it compares the counter first and then the names) we can find the most and least rated genres.

Task 2.6
--------

Without broadcasting, we can collect into a set the `requiredGenres` and then filter out from the movies all the entries that don't match those genres. The set is used as to have efficient lookups operations (O(1)).

Task 2.7
--------

As the previous task, but without using the collect we can broadcast the `requiredGenres` using the callback and then take efficiently its value.

Milestone 2
===========
Task 3.1
--------

The `init()` function in the `Aggregator` is used mainly to store the average rating for each movie. However, as this has to support new updates, we don't store it as plain average, but rather as sum of ratings and count of ratings (tuple `(Double, Int)`). It makes possible this way to extend the average rating upon the insertion of new ratings either to replace older ones or for completely new ones. The RDD containing such information is `averageRatingByMovie: RDD[(Int, (String, (Double, Int), List[String]))]`. That RDD keeps both the title and the list of keywords in it as they're information used by the next methods, avoiding unnecessary joins.

We first compute the average ratings as RDD of tuple `(sum, count)` for each movie. Subsequently, we compute a left outer join between the movie RDD and that average ratings RDD. The join performed is of "left outer" type as non-matching keys are mapped with an empty Option (`None`), thus this can be used to force the rating to be 0 (the RDD will contain all the movies, even the not rated ones).

Finally, we partition by key the RDD and we persist it.

The `getResult()` simply aggregates the information by computing the average (sum divided by count, if count is not zero)

Task 3.2
--------

Initially, we filter from the `averageRatingByMovie` all the movies which have *all* the keywords passed as parameters. If none does, then the method returns `-1`, otherwise we compute the average on fly as in `getResult` and wer return the global mean.

Task 3.3
--------

For efficiency sake we parallelize the parameter `delta_` into an RDD, and then we distinguish two cases:

1. the rating is a compeltely new one (the `Option[Double]` is `None`): then we map it as `(new_rating, 1)`, as this quantity will be summed to the previous ones in the next step of aggregation, and this has to count as a unique rating.
2. the user has already rated that movie before (the `Option[Double]` is `Some`): then we map it as the difference between new and old ratings and as a 0 (the new rating has to replace the old one, not adding on top of it): `(new_rating - old_rating, 0)`

Moreover, the RDD is reduced by key such that for each movie ID we store the total sum of the ratings and the relative count. Finally, we aggregate this result with the previous average ratings (`averageRatingByMovie`) by performing a left outer join. The old RDD is unpersisted, while the new is partitioned by movie ID and persisted.

Milestone 3
===========
Task 4.1
--------

To avoid unnecessary computations, the buckets are computed at the beginning in the class constructor, simply by hashing the list of keywords through the `MinHash` for each entry of the title RDD and then grouping by the hash.
Also, the RDD (`bucketedRDD`) is partitioned by the hash and persisted in memory for future uses.

Task 4.2
--------

The lookup function of the `LSHIndex` takes the queries (as RDD of list of signatures and a payload) and perform a left outer join with the `bucketRDD`, such that signatures contained in the queries but not matching with the pre-computed ones are marked with an empy `Option` and an empty list is returned in that case.

The `NNLookup` object hashes the queries (as RDD of list of keywords) to obtain the signatures of each list and performs an `LSHIndex` lookup (the payload is actually the list of keywords).

Task 4.3
--------

The `init()` function in the `BaselinePredictor` is used to pre-process the data and generate 3 RDDs:

1. `avgRatingByUserId`: the average rating for each user ID .(`RDD[(Int, Double)]`)
2. `globalAvgDeviation`: the global average deviation for each movie ID. (`RDD[(Int, Double)]`)
3. `allRatings`: the collection of all the ratings to get the mean afterwards. (`RDD[Double]`)

In particular, the global average deviation is computed by first getting the normalized deviation, computing the average of it for each movie. That deviation is an RDD mapping the user and the movie to its relative deviation. A join with `avgRatingByUserId` is used to avoid collecting the ratings and taking them in memory, but rather mapping them in the RDD.

Finally, the RDDs are partitioned by their keys and persisted in memory.

The `predict` method simply filters from the RDDs the entries with the specific user ID and movie ID and then returns the result via the given formula.

Task 4.4
--------

Our custom rating RDD is converted into a `Rating` object contained in the library `org.apache.spark.mllib.recommendation` and that is fed into the ALS model (`org.apache.spark.mllib.recommendation.MatrixFactorizationModel`) to train. 
The prediction simply uses the library `predict` method.

Task 4.5
--------

The recommender works identically for both the baseline and the collaborative predictors, only the predict call is different. For this reason, I decided to create a function named `recommend` to which I pass besides the normal parameters a callback to call the right predictor, so it's well parametrized. 
The `recommend` starts by finding the movies that the given user has already seen, as these should be filtered out from the ones looked up, before the prediction.

Finally, the similar movies obtained from the `LSHIndex` lookup and filtered are sorted in a descending way based on their rating and the top `K` are returned as a list.

<!-- Markdeep: -->
<script src="https://morgan3d.github.io/markdeep/latest/markdeep.min.js?" charset="utf-8"></script>
<script>window.alreadyProcessedMarkdeep||(document.body.style.visibility="visible")</script>
