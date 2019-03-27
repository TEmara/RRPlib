


package szu.bdi.generators

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import szu.bdi.rdd.{MultivariateRDDs, RandomRDDs}

object RegressionData {

  // for Normal distribution
  /**
    * Generate an RDD containing test data for Regression with normal distribution N(0,1).
    *
    * @param sc SparkContext to use for creating the RDD
    * @param numRecords Number of points that will be contained in the RDD
    * @param numFeatures Number of dimensions
    * @param numPartitions Number of partitions of the generated RDD; default 2
    * @param f The function that represents the label formula
    * @param repeatFactor factor of repeating the generation process;
    *                     the totall number of the generated records =  repeatFactor * numRecords;
    *                     the totall number of the RDD partitions = repeatFactor * numPartitions;
    *                     default 1
    */
  def NormalRegData( sc :SparkContext,
                     numRecords :Long,
                     numFeatures :Int,
                     f: Array[Double] => Double,
                     numPartitions :Int =2,
                     repeatFactor :Int =1): RDD[Array[Double]] = {

    def generateHelper(rdd: org.apache.spark.rdd.RDD[Array[Double]], repeatIndex: Int): RDD[Array[Double]]={
      if (repeatIndex <= 0) rdd//.map(x=> x.mkString(",")).saveAsTextFile(FinalPath)
      else{
        val rand = scala.util.Random
        val y = RandomRDDs.normalVectorRDD(sc, numRecords, numFeatures, numPartitions).map {v =>
          v.toArray :+ f(v.toArray)
        }
        generateHelper(rdd ++ y, repeatIndex - 1)
      }
    }
    generateHelper(sc.emptyRDD[Array[Double]],repeatFactor)
  }

  // for StudentT distribution
  /**
    * Generate an RDD containing test data for Regression with Student's T-distribution.
    *
    * @param sc SparkContext to use for creating the RDD
    * @param degreesOfFreedom degrees of freedom for the Student's T-distribution.
    * @param numRecords Number of points that will be contained in the RDD
    * @param numFeatures Number of dimensions
    * @param numPartitions Number of partitions of the generated RDD; default 2
    * @param f The function that represents the label formula
    * @param repeatFactor factor of repeating the generation process;
    *                     the totall number of the generated records =  repeatFactor * numRecords;
    *                     the totall number of the RDD partitions = repeatFactor * numPartitions;
    *                     default 1
    */
  def TRegData( sc :SparkContext,
                degreesOfFreedom: Double,
                numFeatures :Int,
                repeatFactor :Int = 1,
                numRecords :Long,
                numPartitions :Int,
                f: Array[Double] => Double ): RDD[Array[Double]] = {

    def generateHelper(rdd: org.apache.spark.rdd.RDD[Array[Double]], repeatIndex: Int): RDD[Array[Double]]={
      if (repeatIndex <= 0) rdd//.map(x=> x.mkString(",")).saveAsTextFile(FinalPath)
      else{
        val rand = scala.util.Random
        val y = RandomRDDs.studentTVectorRDD(sc, degreesOfFreedom, numRecords, numFeatures, numPartitions).map {v =>
          v.toArray :+ f(v.toArray)
        }
        generateHelper(rdd ++ y, repeatIndex - 1)
      }
    }
    generateHelper(sc.emptyRDD[Array[Double]],repeatFactor)
  }

  // for Gamma distribution
  def GammaRegData( sc :SparkContext,
                    numRecords :Long,
                    numFeatures :Int,
                    shapes: Array[Double],
                    scales: Array[Double],
                    f: Array[Double] => Double,
                    numPartitions :Int = 2,
                    repeatFactor :Int = 1 ): RDD[Array[Double]] = {

    def generateHelper(rdd: org.apache.spark.rdd.RDD[Array[Double]], repeatIndex: Int): RDD[Array[Double]]={
      if (repeatIndex <= 0) rdd//.map(x=> x.mkString(",")).saveAsTextFile(FinalPath)
      else{
        val y = MultivariateRDDs.gammaVectorRDD(sc, shapes, scales, numRecords, numFeatures, numPartitions).map {v =>
          v.toArray :+ f(v.toArray)
        }
        generateHelper(rdd ++ y, repeatIndex - 1)
      }
    }
    generateHelper(sc.emptyRDD[Array[Double]],repeatFactor)
  }
}
