package szu.bdi.generators

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import szu.bdi.rdd.{MultivariateRDDs, RandomRDDs}

import scala.util.Random

object ClassificationData {

  /**
    * Generate an RDD containing test data for Classification with normal distribution N(u,s) where u in U(0,10)and s in U(0,10), and
    * noise features U(0,10)
    *
    * @param sc SparkContext to use for creating the RDD
    * @param DSPath The dataset path
    * @param numRecords Number of points that will be contained in the RDD
    * @param numFeatures Number of dimensions
    * @param numPartitions Number of partitions of the generated RDD; default 2
    * @param noiseFeature Number of noise features
    * @param iClassCount  The class label
    * @param repeatFactor factor of repeating the generation process;
    *                     the totall number of the generated records =  repeatFactor * numRecords;
    *                     the totall number of the RDD partitions = repeatFactor * numPartitions;
    *                     default 1
    */
  def generateOneClass( sc :SparkContext,
                DSPath: String,
                numFeatures :Int,
                repeatFactor :Int,
                numRecords :Long,
                numPartitions :Int,
                iClassCount:Int,
                noiseFeature: Int): Unit = {

    var UnRDD: org.apache.spark.rdd.RDD[Array[Double]] = sc.emptyRDD
    val means: Vector[Int] = Vector.fill(numFeatures)(new Random(Random.nextLong).nextInt(10))
    val sigmas: Vector[Double] = Vector.fill(numFeatures)(new Random(Random.nextLong).nextInt(10))

    for (rF <- 0 until repeatFactor) {
      val y = RandomRDDs.normalVectorRDD(sc, numRecords, numFeatures - noiseFeature, numPartitions).map { v =>
        var res = new Array[Double](0)
        for (i <- 0 until numFeatures - noiseFeature) {
          def x = means(i) + sigmas(i) * v(i)

          res = res :+ BigDecimal(x).setScale(7, BigDecimal.RoundingMode.HALF_UP).toDouble
        }
        val random = new Random(Random.nextLong)
        for (i <- numFeatures - noiseFeature until numFeatures) {
          //res = res:+ random.nextDouble()*random.nextInt(10)
          res = res :+ random.nextInt(100) * 1.0
        }
        res = res :+ iClassCount.toDouble //((65+i).toChar.toString)
        res
      }
      UnRDD = UnRDD ++ y
    }
    UnRDD.map(x=> x.mkString(",")).saveAsTextFile(DSPath + "/DS" + iClassCount)
  }

  def generateMultiClasses( sc :SparkContext,
                            hdfs :FileSystem,
                            DSPath: String,
                            numFeatures :Int,
                            numRecords :Long,
                            numPartitions :Int,
                            ClassCount:Int,
                            shiftFactor: Int = 0,
                            repeatFactor :Int = 1,
                            noiseFeature: Int = 0): Unit = {
    val threads =
      for (i <- (1 to ClassCount).toList)
        yield new Thread(new Runnable with Serializable {
          override def run(): Unit = {
            generateOneClass(sc, DSPath, numFeatures, repeatFactor, numRecords, numPartitions, i + shiftFactor, noiseFeature)
            filesCollect(hdfs, DSPath, i + shiftFactor)
          }
        })

    threads.foreach(t => t.start())
    threads.foreach(t => t.join())
  }
  def filesCollect( hdfs :FileSystem,
                    DestPath :String,
                    ClassCount:Int): Unit = {
    val fileListDir = hdfs.listStatus(new Path(DestPath + "/DS" + ClassCount))
    val df = new java.text.DecimalFormat("000000")
    for (i <- 1 until fileListDir.length){
      val ok = hdfs.rename(fileListDir(i).getPath, new Path(DestPath + "/part" + ClassCount + "-" + df.format(i)))
    }
    println("Generating Class " + ClassCount +" done")
    hdfs.delete(new Path(DestPath + "/DS" + ClassCount),true)
  }
}
