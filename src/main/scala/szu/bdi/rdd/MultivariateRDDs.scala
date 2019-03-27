/*
* Developed by Tamer Emara
* On April 1, 2018
*/

package szu.bdi.rdd

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import szu.bdi.random._
import szu.bdi.utils.Utils


object MultivariateRDDs {
  // TODO Generate RDD[Vector] from multivariate distributions.

  /**
    * Generates an RDD[Vector] with vectors containing `i.i.d.` samples drawn from the
    * uniform distribution on `U(0.0, 1.0)`.
    *
    * @param sc SparkContext used to create the RDD.
    * @param numRows Number of Vectors in the RDD.
    * @param numCols Number of elements in each Vector.
    * @param numPartitions Number of partitions in the RDD.
    * @param seed Seed for the RNG that generates the seed for the generator in each partition.
    * @return RDD[Vector] with vectors containing i.i.d samples ~ `U(a, b)`.
    */
  def uniformVectorRDD(
                        sc: SparkContext,
                        a: Array[Double],
                        b: Array[Double],
                        numRows: Long,
                        numCols: Int,
                        numPartitions: Int = 0,
                        seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    require(a.length == numCols, "The size of lower bound array must be equal to the number of numCols.")
    require(b.length == numCols, "The size of upper bound array must be equal to the number of numCols.")

    val uniform = new Array[RandomDataGenerator[Double]](numCols)
    for (i <- 0 until numCols){
      uniform(i) = new UniformGeneratorWithBound(a(i),b(i))
    }
    multivariateRDD(sc, uniform, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
    * Generates an RDD[Vector] with vectors containing `i.i.d.` samples drawn from the
    * standard normal distribution.
    *
    * @param sc SparkContext used to create the RDD.
    * @param numRows Number of Vectors in the RDD.
    * @param numCols Number of elements in each Vector.
    * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
    * @param seed Random seed (default: a random long integer).
    * @return RDD[Vector] with vectors containing `i.i.d.` samples ~ `N(0.0, 1.0)`.
    */
  def normalVectorRDD(
                       sc: SparkContext,
                       means: Array[Double],
                       stds: Array[Double],
                       numRows: Long,
                       numCols: Int,
                       numPartitions: Int = 0,
                       seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    require(means.length == numCols, "The size of means array must be equal to the number of numCols.")
    require(stds.length == numCols, "The size of stds array must be equal to the number of numCols.")

    val normal = new Array[RandomDataGenerator[Double]](numCols)
    for (i <- 0 until numCols){
      normal(i) = new NormalGenerator(means(i),stds(i))
    }
    multivariateRDD(sc, normal, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
    * Generates an RDD[Vector] with vectors containing `i.i.d.` samples drawn from a
    * log normal distribution.
    *
    * @param sc SparkContext used to create the RDD.
    * @param means Mean of the log normal distribution.
    * @param stds Standard deviation of the log normal distribution.
    * @param numRows Number of Vectors in the RDD.
    * @param numCols Number of elements in each Vector.
    * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
    * @param seed Random seed (default: a random long integer).
    * @return RDD[Vector] with vectors containing `i.i.d.` samples.
    */
  def logNormalVectorRDD(
                          sc: SparkContext,
                          means: Array[Double],
                          stds: Array[Double],
                          numRows: Long,
                          numCols: Int,
                          numPartitions: Int = 0,
                          seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    require(means.length == numCols, "The size of means array must be equal to the number of numCols.")
    require(stds.length == numCols, "The size of stds array must be equal to the number of numCols.")

    val logNormal = new Array[RandomDataGenerator[Double]](numCols)
    for (i <- 0 until numCols){
      logNormal(i) = new LogNormalGenerator(means(i),stds(i))
    }
    multivariateRDD(sc, logNormal, numRows, numCols,
      numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
    * Generates an RDD[Vector] with vectors containing `i.i.d.` samples drawn from the
    * Poisson distribution with the input mean.
    *
    * @param sc SparkContext used to create the RDD.
    * @param means Mean, or lambda, for the Poisson distribution.
    * @param numRows Number of Vectors in the RDD.
    * @param numCols Number of elements in each Vector.
    * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`)
    * @param seed Random seed (default: a random long integer).
    * @return RDD[Vector] with vectors containing `i.i.d.` samples ~ Pois(mean).
    */
  def poissonVectorRDD(
                        sc: SparkContext,
                        means: Array[Double],
                        numRows: Long,
                        numCols: Int,
                        numPartitions: Int = 0,
                        seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    require(means.length == numCols, "The size of means array must be equal to the number of numCols.")

    val poisson = new Array[RandomDataGenerator[Double]](numCols)
    for (i <- 0 until numCols){
      poisson(i) = new PoissonGenerator(means(i))
    }
    multivariateRDD(sc, poisson, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
    * Generates an RDD[Vector] with vectors containing `i.i.d.` samples drawn from the
    * exponential distribution with the input mean.
    *
    * @param sc SparkContext used to create the RDD.
    * @param means Mean, or 1 / lambda, for the Exponential distribution.
    * @param numRows Number of Vectors in the RDD.
    * @param numCols Number of elements in each Vector.
    * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`)
    * @param seed Random seed (default: a random long integer).
    * @return RDD[Vector] with vectors containing `i.i.d.` samples ~ Exp(mean).
    */
  def exponentialVectorRDD(
                            sc: SparkContext,
                            means: Array[Double],
                            numRows: Long,
                            numCols: Int,
                            numPartitions: Int = 0,
                            seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    require(means.length == numCols, "The size of means array must be equal to the number of numCols.")

    val exponential = new Array[RandomDataGenerator[Double]](numCols)
    for (i <- 0 until numCols){
      exponential(i) = new ExponentialGenerator(means(i))
    }
    multivariateRDD(sc, exponential, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
    * Generates an RDD[Vector] with vectors containing `i.i.d.` samples drawn from the
    * gamma distribution with the input shape and scale.
    *
    * @param sc SparkContext used to create the RDD.
    * @param shapes shape parameter (greater than 0) for the gamma distribution.
    * @param scales scale parameter (greater than 0) for the gamma distribution.
    * @param numRows Number of Vectors in the RDD.
    * @param numCols Number of elements in each Vector.
    * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`)
    * @param seed Random seed (default: a random long integer).
    * @return RDD[Vector] with vectors containing `i.i.d.` samples ~ Exp(mean).
    */
  def gammaVectorRDD(
                      sc: SparkContext,
                      shapes: Array[Double],
                      scales: Array[Double],
                      numRows: Long,
                      numCols: Int,
                      numPartitions: Int = 0,
                      seed: Long = Utils.random.nextLong()): RDD[Vector] = {

    require(shapes.length == numCols, "The size of shapes array must be equal to the number of numCols.")
    require(scales.length == numCols, "The size of scales array must be equal to the number of numCols.")

    val gamma = new Array[RandomDataGenerator[Double]](numCols)
    for (i <- 0 until numCols){
      gamma(i) = new GammaGenerator(shapes(i), scales(i))
    }
    multivariateRDD(sc, gamma, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
    * :: DeveloperApi ::
    * Generates an RDD[Vector] with vectors containing `i.i.d.` samples produced by the
    * input RandomDataGenerator.
    *
    * @param sc SparkContext used to create the RDD.
    * @param generator RandomDataGenerator used to populate the RDD.
    * @param numRows Number of Vectors in the RDD.
    * @param numCols Number of elements in each Vector.
    * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
    * @param seed Random seed (default: a random long integer).
    * @return RDD[Vector] with vectors containing `i.i.d.` samples produced by generator.
    */
  def multivariateRDD(sc: SparkContext,
                      generator: Array[RandomDataGenerator[Double]],
                      numRows: Long,
                      numCols: Int,
                      numPartitions: Int = 0,
                      seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    new MultivariateRDD(
      sc, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), generator, seed)
  }

  /**
    * Returns `numPartitions` if it is positive, or `sc.defaultParallelism` otherwise.
    */
  private def numPartitionsOrDefault(sc: SparkContext, numPartitions: Int): Int = {
    if (numPartitions > 0) numPartitions else sc.defaultMinPartitions
  }


  /**
    * Generates an RDD[Vector] with vectors containing `i.i.d.` samples drawn from the
    * Student's T-distribution with the input mean.
    *
    * @param sc SparkContext used to create the RDD.
    * @param degreesOfFreedom degrees of freedom for the Student's T-distribution.
    * @param numRows Number of Vectors in the RDD.
    * @param numCols Number of elements in each Vector.
    * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`)
    * @param seed Random seed (default: a random long integer).
    * @return RDD[Vector] with vectors containing `i.i.d.` samples ~ Pois(mean).
    */
  def studentTVectorRDD(
                         sc: SparkContext,
                         degreesOfFreedom: Array[Double],
                         numRows: Long,
                         numCols: Int,
                         numPartitions: Int = 0,
                         seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    require(degreesOfFreedom.length == numCols, "The size of degrees of freedom array must be equal to the number of numCols.")
    val studentT = new Array[RandomDataGenerator[Double]](numCols)
    for (i <- 0 until numCols){
      studentT(i) = new StudentTGenerator(degreesOfFreedom(i))
    }
    multivariateRDD(sc, studentT, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), seed)
  }
}
