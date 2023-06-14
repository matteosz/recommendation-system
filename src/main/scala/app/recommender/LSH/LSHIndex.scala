package app.recommender.LSH


import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Class for indexing the data for LSH
 *
 * @param data The title data to index
 * @param seed The seed to use for hashing keyword lists
 */
class LSHIndex(data: RDD[(Int, String, List[String])], seed: IndexedSeq[Int]) extends Serializable {

  private val minhash = new MinHash(seed)

  // RDD that for each hash maps the movies which have the list of keywords matching that hash
  // Pre-compute it in the constructor
  private var bucketedRDD: RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] =
  data.map(x => (minhash.hash(x._3), x))
    .groupByKey()
    .mapValues(_.toList)

  // Partition and persist the RDD
  private val partitioner: HashPartitioner = new HashPartitioner(bucketedRDD.partitions.length)
  bucketedRDD = bucketedRDD.partitionBy(partitioner).persist()

  /**
   * Hash function for an RDD of queries.
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (signature, keyword list) pairs
   */
  def hash(input: RDD[List[String]]): RDD[(IndexedSeq[Int], List[String])] =
    input.map(x => (minhash.hash(x), x))

  /**
   * Return data structure of LSH index for testing
   *
   * @return Data structure of LSH index
   */
  def getBuckets(): RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = bucketedRDD

  /**
   * Lookup operation on the LSH index
   *
   * @param queries The RDD of queries. Each query contains the pre-computed signature
   *                and a payload
   * @return The RDD of (signature, payload, result) triplets after performing the lookup.
   *         If no match exists in the LSH index, return an empty result list.
   */
  def lookup[T: ClassTag](queries: RDD[(IndexedSeq[Int], T)])
  : RDD[(IndexedSeq[Int], T, List[(Int, String, List[String])])] = {
    // Perform a left outer join such not matching tuples
    // return an optional None
    queries.leftOuterJoin(bucketedRDD)
      .map(x => {
        if (x._2._2.isEmpty) {
          // Return empty list if not matching that hash
          (x._1, x._2._1, List.empty)
        } else {
          (x._1, x._2._1, x._2._2.get)
        }
      })
  }
}
