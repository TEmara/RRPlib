package szu.bdi.apps

import org.apache.spark.sql.SparkSession
import szu.bdi.ConfigObjDev
import szu.bdi.partitioner._

object partitionerApp {
  def main(args: Array[String]):Unit =  {
    if (args.length < 3) {
      println("Usage: spark-submit --class szu.bdi.apps.partitionerApp RRP.jar <iDS> <oDS> <Q>")
      println("<iDS>: Input dataset name.")
      println("<oDS>: Output dataset name.")
      println("<Q>: Number of partitions for the output dataset.")
      sys.exit()
    }

    val spark = SparkSession.builder
      .master("yarn")
      .appName("Partition: " + args(0) + " -> " + args(1))
      .config(conf = ConfigObjDev.sparkConf)
      .getOrCreate()

    val sourcePath: String = args(0) //"/user/tamer/DS0050_1"
    val FinalPath: String = args(1) //"/user/tamer/RDS0050_2"
    val NumPartitionsRequired = args(2).toInt //1000

    val sc = spark.sparkContext
    val datasetRDD = sc.textFile(sourcePath)

    RRP(datasetRDD).partitionByRRP(NumPartitionsRequired).saveAsTextFile(FinalPath)

    spark.stop()
  }
}
