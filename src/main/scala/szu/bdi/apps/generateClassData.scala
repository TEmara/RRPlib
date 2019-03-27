package szu.bdi.apps

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import szu.bdi.generators.ClassificationData.generateMultiClasses

object generateClassData {
  def main(args: Array[String]) {
    if (args.length < 5){
      println("\t Usage: spark-submit GenerateDS.jar <oDS> <R> <F> <K> <P> [<sh> <nf> <r>]")
      println("\t <oDS>: dataset name.")
      println("\t <R>: Number of records. ")
      println("\t <F>: Number of features.")
      println("\t <K>: Number of clusters.")
      println("\t <P>: Number of partitions.")
      println("\t <sh>: Starting number of clusters. (default 0)")
      println("\t <nf>: Number of noise features (default 0).")
      println("\t <r>: Repeat factor (default 1).")
      System.exit(1)
    }
    val conf: SparkConf = new SparkConf()
      .setAppName("Generate_DataSet " + args(0))
      .setMaster("yarn")
      .set("spark.submit.deployMode","client")
      .set("spark.executor.memory", "25g")// 10
      .set("spark.executor.cores", "16")// 5
      .set("spark.executor.instances", "43")//20 149

    val sc = new SparkContext(conf)
    val FinalPath: String = args(0)
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(hadoopConf)
    val ClassCount: Int = args(3).toInt // K
    val numRecords: Long = args(1).toLong / ClassCount // R/K
    //real total record no. = recordCount * ClassCount * repeatFactor
    val repeatFactor: Int = if (args.length > 7) args(7).toInt else 1
    val numFeatures: Int = args(2).toInt
    val shiftFactor = if (args.length > 5) args(5).toInt else 0
    val noiseFeature = if (args.length > 6) args(6).toInt else 0
    val numPartitions: Int = if((args(4).toInt / ClassCount)>1) args(4).toInt / ClassCount else 1
    // real total number of partitions = numPartitions * ClassCount * repeatFactor

    hdfs.mkdirs(new Path(FinalPath))
    generateMultiClasses(
      sc,
      hdfs,
      FinalPath,
      numFeatures,
      numRecords,
      numPartitions,
      ClassCount,
      shiftFactor,
      repeatFactor,
      noiseFeature)

    println("all done")
  }



}
