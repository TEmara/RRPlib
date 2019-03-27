/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package szu.bdi.rdd

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import szu.bdi.random._
import szu.bdi.utils.Utils

import scala.reflect.ClassTag

/**
 * Generator methods for creating RDDs comprised of `i.i.d.` samples from some distribution.
 */
object RandomRDDs {

  /**
   * Generates an RDD comprised of `i.i.d.` samples from the uniform distribution `U(0.0, 1.0)`.
   *
   * To transform the distribution in the generated RDD from `U(0.0, 1.0)` to `U(a, b)`, use
   * `RandomRDDs.uniformRDD(sc, n, p, seed).map(v => a + (b - a) * v)`.
   *
   * @param sc SparkContext used to create the RDD.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Double] comprised of `i.i.d.` samples ~ `U(0.0, 1.0)`.
   */
  def uniformRDD(
      sc: SparkContext,
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Double] = {
    val uniform = new UniformGenerator()
    randomRDD(sc, uniform, size, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Generates an RDD comprised of `i.i.d.` samples from the standard normal distribution.
   *
   * To transform the distribution in the generated RDD from standard normal to some other normal
   * `N(mean, sigma^2^)`, use `RandomRDDs.normalRDD(sc, n, p, seed).map(v => mean + sigma * v)`.
   *
   * @param sc SparkContext used to create the RDD.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Double] comprised of `i.i.d.` samples ~ N(0.0, 1.0).
   */
  def normalRDD(
      sc: SparkContext,
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Double] = {
    val normal = new StandardNormalGenerator()
    randomRDD(sc, normal, size, numPartitionsOrDefault(sc, numPartitions), seed)
  }

   /**
   * Generates an RDD comprised of `i.i.d.` samples from the Poisson distribution with the input
   * mean.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or lambda, for the Poisson distribution.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Double] comprised of `i.i.d.` samples ~ Pois(mean).
   */
  def poissonRDD(
      sc: SparkContext,
      mean: Double,
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Double] = {
    val poisson = new PoissonGenerator(mean)
    randomRDD(sc, poisson, size, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Generates an RDD comprised of `i.i.d.` samples from the exponential distribution with
   * the input mean.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or 1 / lambda, for the exponential distribution.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Double] comprised of `i.i.d.` samples ~ Pois(mean).
   */
  def exponentialRDD(
      sc: SparkContext,
      mean: Double,
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Double] = {
    val exponential = new ExponentialGenerator(mean)
    randomRDD(sc, exponential, size, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Generates an RDD comprised of `i.i.d.` samples from the gamma distribution with the input
   *  shape and scale.
   *
   * @param sc SparkContext used to create the RDD.
   * @param shape shape parameter (greater than 0) for the gamma distribution
   * @param scale scale parameter (greater than 0) for the gamma distribution
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Double] comprised of `i.i.d.` samples ~ Pois(mean).
   */
  def gammaRDD(
      sc: SparkContext,
      shape: Double,
      scale: Double,
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Double] = {
    val gamma = new GammaGenerator(shape, scale)
    randomRDD(sc, gamma, size, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Generates an RDD comprised of `i.i.d.` samples from the log normal distribution with the input
   *  mean and standard deviation
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean mean for the log normal distribution
   * @param std standard deviation for the log normal distribution
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Double] comprised of `i.i.d.` samples ~ Pois(mean).
   */
  def logNormalRDD(
      sc: SparkContext,
      mean: Double,
      std: Double,
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Double] = {
    val logNormal = new LogNormalGenerator(mean, std)
    randomRDD(sc, logNormal, size, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Generates an RDD comprised of `i.i.d.` samples produced by the input RandomDataGenerator.
   *
   * @param sc SparkContext used to create the RDD.
   * @param generator RandomDataGenerator used to populate the RDD.
   * @param size Size of the RDD.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[T] comprised of `i.i.d.` samples produced by generator.
   */
  def randomRDD[T: ClassTag](
      sc: SparkContext,
      generator: RandomDataGenerator[T],
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[T] = {
    new RandomRDD[T](sc, size, numPartitionsOrDefault(sc, numPartitions), generator, seed)
  }


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
   * @return RDD[Vector] with vectors containing i.i.d samples ~ `U(0.0, 1.0)`.
   */
  def uniformVectorRDD(
      sc: SparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    val uniform = new UniformGenerator()
    randomVectorRDD(sc, uniform, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), seed)
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
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    val normal = new StandardNormalGenerator()
    randomVectorRDD(sc, normal, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Generates an RDD[Vector] with vectors containing `i.i.d.` samples drawn from a
   * log normal distribution.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean of the log normal distribution.
   * @param std Standard deviation of the log normal distribution.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Vector] with vectors containing `i.i.d.` samples.
   */
  def logNormalVectorRDD(
      sc: SparkContext,
      mean: Double,
      std: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    val logNormal = new LogNormalGenerator(mean, std)
    randomVectorRDD(sc, logNormal, numRows, numCols,
      numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Generates an RDD[Vector] with vectors containing `i.i.d.` samples drawn from the
   * Poisson distribution with the input mean.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or lambda, for the Poisson distribution.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`)
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Vector] with vectors containing `i.i.d.` samples ~ Pois(mean).
   */
  def poissonVectorRDD(
      sc: SparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    val poisson = new PoissonGenerator(mean)
    randomVectorRDD(sc, poisson, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Generates an RDD[Vector] with vectors containing `i.i.d.` samples drawn from the
   * exponential distribution with the input mean.
   *
   * @param sc SparkContext used to create the RDD.
   * @param mean Mean, or 1 / lambda, for the Exponential distribution.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`)
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Vector] with vectors containing `i.i.d.` samples ~ Exp(mean).
   */
  def exponentialVectorRDD(
      sc: SparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    val exponential = new ExponentialGenerator(mean)
    randomVectorRDD(sc, exponential, numRows, numCols,
      numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
   * Generates an RDD[Vector] with vectors containing `i.i.d.` samples drawn from the
   * gamma distribution with the input shape and scale.
   *
   * @param sc SparkContext used to create the RDD.
   * @param shape shape parameter (greater than 0) for the gamma distribution.
   * @param scale scale parameter (greater than 0) for the gamma distribution.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`)
   * @param seed Random seed (default: a random long integer).
   * @return RDD[Vector] with vectors containing `i.i.d.` samples ~ Exp(mean).
   */
  def gammaVectorRDD(
      sc: SparkContext,
      shape: Double,
      scale: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    val gamma = new GammaGenerator(shape, scale)
    randomVectorRDD(sc, gamma, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
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
  def randomVectorRDD(sc: SparkContext,
      generator: RandomDataGenerator[Double],
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    new RandomVectorRDD(
      sc, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), generator, seed)
  }

  /**
   * Returns `numPartitions` if it is positive, or `sc.defaultParallelism` otherwise.
   */
  private def numPartitionsOrDefault(sc: SparkContext, numPartitions: Int): Int = {
    if (numPartitions > 0) numPartitions else sc.defaultMinPartitions
  }


  /**
    * Developed by Tamer Emara
    * On April 1, 2018
    *
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
                        degreesOfFreedom: Double,
                        numRows: Long,
                        numCols: Int,
                        numPartitions: Int = 0,
                        seed: Long = Utils.random.nextLong()): RDD[Vector] = {
    val studentT = new StudentTGenerator(degreesOfFreedom)
    randomVectorRDD(sc, studentT, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), seed)
  }

  /**
    * Developed by Tamer Emara
    * On April 1, 2018
    *
    * Generates an RDD comprised of `i.i.d.` samples from the Student's T-distribution with the input
    * mean.
    *
    * @param sc SparkContext used to create the RDD.
    * @param degreesOfFreedom degrees of freedom for the Student's T-distribution..
    * @param size Size of the RDD.
    * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
    * @param seed Random seed (default: a random long integer).
    * @return RDD[Double] comprised of `i.i.d.` samples ~ Pois(mean).
    */
  def studentTRDD(
                  sc: SparkContext,
                  degreesOfFreedom: Double,
                  size: Long,
                  numPartitions: Int = 0,
                  seed: Long = Utils.random.nextLong()): RDD[Double] = {
    val studentT = new StudentTGenerator(degreesOfFreedom)
    randomRDD(sc, studentT, size, numPartitionsOrDefault(sc, numPartitions), seed)
  }

}
