package szu.bdi.generators

import com.fasterxml.jackson.annotation.JsonFormat.Shape
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import szu.bdi.random.GammaGenerator
import szu.bdi.rdd.{RandomRDDs, MultivariateRDDs}
//import org.apache.spark.mllib.random.RandomRDDs

object GenerateRegressionData {


  def GenerateRegressionData ( sc :SparkContext,
                               featureCount :Int,
                               repeatFactor :Int,
                               recordCount :Long,
                               numPartitions :Int,
                               FinalPath :String
                             ): Unit = {
    //val y:Array[org.apache.spark.rdd.RDD[scala.collection.immutable.Vector[Any]]]=new Array[org.apache.spark.rdd.RDD[scala.collection.immutable.Vector[Any]]](repeatFactor)
    var UnRDD :org.apache.spark.rdd.RDD[scala.collection.immutable.Vector[Any]]=sc.emptyRDD[scala.collection.immutable.Vector[Any]]

    for(rF <- 0 until repeatFactor){
      val y = RandomRDDs.uniformVectorRDD(sc, recordCount, featureCount, numPartitions).map {v =>
        var res :Vector[Any]= scala.collection.immutable.Vector.empty
        var f :Double = 0
        for (i <- 0 until featureCount){
          def x :Any = v(i)
          res = res:+ x
          f += (v(i)*v(i))
        }
        res = res:+ f
        res
      }
      UnRDD = UnRDD ++ y
    }
    UnRDD.map(x=> x.toArray.mkString(",")).saveAsTextFile(FinalPath)
  }

  def generateNormalRDD( sc :SparkContext,
                         featuresCount :Int,
                         recordsCount :Long,
                         numPartitions :Int,
                         repeatFactor :Int
                       ): RDD[Array[Double]] = {

    def generateHelper(rdd: org.apache.spark.rdd.RDD[Array[Double]], repeatIndex: Int): RDD[Array[Double]]={
      if (repeatIndex <= 0) rdd//.map(x=> x.mkString(",")).saveAsTextFile(FinalPath)
      else{
        val rand = scala.util.Random
        val y = RandomRDDs.normalVectorRDD(sc, recordsCount, featuresCount, numPartitions).map {v =>
          val sum = v.toArray.sum
          val epsilon = rand.nextGaussian()*10
          v.toArray :+ (sum + epsilon)
        }
        generateHelper(rdd ++ y, repeatIndex - 1)
      }
    }
    generateHelper(sc.emptyRDD[Array[Double]],repeatFactor)
  }

  def generateStudentTRDD( sc :SparkContext,
                       degreesOfFreedom: Double,
                       featuresCount :Int,
                       repeatFactor :Int,
                       recordsCount :Long,
                       numPartitions :Int
                     ): RDD[Array[Double]] = {

    def generateHelper(rdd: org.apache.spark.rdd.RDD[Array[Double]], repeatIndex: Int): RDD[Array[Double]]={
      if (repeatIndex <= 0) rdd//.map(x=> x.mkString(",")).saveAsTextFile(FinalPath)
      else{
        val rand = scala.util.Random
        val y = RandomRDDs.studentTVectorRDD(sc, degreesOfFreedom, recordsCount, featuresCount, numPartitions).map {v =>
          val sum = v.toArray.sum
          val epsilon = rand.nextGaussian()*10
          v.toArray :+ (sum + epsilon)
        }
        generateHelper(rdd ++ y, repeatIndex - 1)
      }
    }
    generateHelper(sc.emptyRDD[Array[Double]],repeatFactor)
  }

  def generateGammaRDD2( sc :SparkContext,
                         numFeatures :Int,
                         numRecords :Long,
                         numPartitions :Int = 2,
                         repeatFactor :Int = 1
                       ): RDD[Array[Double]] = {
    // shape = (1 + 5 (j - 1)) / max(numFeatures-1 , 1) where j is the column number
    def shapes: Array[Double] = {
      val res = new Array[Double](numFeatures)
      for (i <- 0 until numFeatures){
        res(i) = 1 + (5 * (i - 1) / math.max(numFeatures - 1, 1))
      }
      res
    }
    //scale = 2
    val scales = Array.fill(numFeatures)(2.0)

    val epsilonGenerator = new GammaGenerator(1, 2)

    def generateHelper(rdd: org.apache.spark.rdd.RDD[Array[Double]], repeatIndex: Int): RDD[Array[Double]]={
      if (repeatIndex <= 0) rdd//.map(x=> x.mkString(",")).saveAsTextFile(FinalPath)
      else{
        val y = MultivariateRDDs.gammaVectorRDD(sc, shapes, scales, numRecords, numFeatures, numPartitions).map {v =>
          val sum = v.toArray.sum
          val epsilon = epsilonGenerator.nextValue() - 2 // epsilon = gamma(1, 2) - 2
          v.toArray :+ (sum + epsilon)
        }
        generateHelper(rdd ++ y, repeatIndex - 1)
      }
    }
    generateHelper(sc.emptyRDD[Array[Double]],repeatFactor)
  }

  /**
    * Generate an RDD containing test data for KMeans.
    *
    * @param sc SparkContext to use for creating the RDD
    * @param shapes
    * @param scales
    * @param numFeatures Number of dimensions
    * @param numRecords Number of points that will be contained in the RDD
    * @param repeatFactor factor for the distribution of the initial centers
    * @param numPartitions Number of partitions of the generated RDD; default 2
    */
  def generateGammaRDD( sc: SparkContext,
                         //shapes: Array[Double],
                         //scales: Array[Double],
                        numFeatures :Int,
                        numRecords :Int,
                        numPartitions: Int = 2,
                        repeatFactor :Int = 1)
  : RDD[Array[Double]] =
  {
    //require(shapes.length == featuresCount, "The size of shapes array must be equal to the number of dimensions.")
    //require(scales.length == featuresCount, "The size of scales array must be equal to the number of dimensions.")
    // shape = (1 + 5 (j - 1)) / max(numFeatures-1 , 1) where j is the column number
    def shapes: Array[Double] = {
      val res = new Array[Double](numFeatures)
      for (i <- 0 until numFeatures){
        res(i) = 1 + (5 * (i - 1) / math.max(numFeatures - 1, 1))
      }
      res
    }
    //scale = 2
    val scales = Array.fill(numFeatures)(2.0)
    val epsilonGenerator = new GammaGenerator(1, 2)

    val gammas = new Array[GammaGenerator](numFeatures)
    for (i <- 0 until numFeatures){
      gammas(i) = new GammaGenerator(shapes(i), scales(i))
    }

    // Then generate points
    sc.parallelize(0 until numRecords, numPartitions).map { idx =>
      Array.tabulate(numFeatures)(i => gammas(i).nextValue() - 2 - (10 * (i - 1) / math.max(numFeatures - 1, 1)))
    }.mapPartitions(it=>{
      it.map({v =>
        val sum = v.sum
        val epsilon = epsilonGenerator.nextValue() - 2
        v :+ (sum + epsilon)
      })
    })
  }
}
