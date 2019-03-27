/*
* This is an example to generate data with the form $ Z_i=(X_{i}, Y_{i})$
* which is independently and identically distributed (iid) for $i = 1, ..., N$.
* The data is synthesized from the linear model $Y_i= X_i^T 1_M+\varepsilon_i$
* with the distribution, $X_{i,j} \sim gamma{1+5(j-1)/max(d-1,1), 2}$
* with $\varepsilon_i \sim gamma(1, 2)-2$,
* */


package szu.bdi.apps

import org.apache.spark.sql.SparkSession
import szu.bdi.ConfigObjDev
import szu.bdi.random.GammaGenerator
import szu.bdi.generators.RegressionData

object generateRegDataWithGamma {
  def main(args: Array[String]) {
    if (args.length < 4){
      println("\t Usage: spark-submit GenerateDS.jar <oDS> <R> <F> <P> [<r>]")
      println("\t <oDS>: dataset name.")
      println("\t <R>: Number of records. ")
      println("\t <F>: Number of features.")
      println("\t <P>: Number of partitions.")
      println("\t <r>: Repeat factor (default 1).")
      System.exit(1)
    }

    val spark = SparkSession.builder
      .master("yarn")
      .appName("Generate Dataset: " + args(0))
      .config(conf = ConfigObjDev.sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext
    val FinalPath: String = args(0)
    val recordsCount: Long = args(1).toLong  // R/K
    val numFeatures: Int = args(2).toInt
    val numPartitions: Int = args(3).toInt  //real total record no. = recordCount * repeatFactor
    val repeatFactor: Int = if (args.length > 4) args(4).toInt else 1

    val epsilonGenerator = new GammaGenerator(1, 2)

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

    def label: Array[Double]=> Double = {
      v =>
        val sum = v.sum
        val epsilon = epsilonGenerator.nextValue() - 2 // epsilon = gamma(1, 2) - 2
        sum + epsilon
    }
    RegressionData.GammaRegData(sc,  recordsCount, numFeatures, shapes, scales, label, numPartitions, repeatFactor)
    .map(x=> x.mkString(",")).saveAsTextFile(FinalPath)
  }
}
