package szu.bdi.apps

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import szu.bdi.ConfigObjDev
import szu.bdi.hdfs.Operations._
import szu.bdi.partitioner._

object massiveRRP_App {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("Usage: spark-submit PartitionAlgorithm.jar <iDS> <oDS> <Q> <d>")
      println("<iDS>: Input dataset name.")
      println("<oDS>: Output dataset name.")
      println("<Q>: Number of partitions for the output dataset.")
      println("<d>: Number of sub-datasets.")
      sys.exit()
    }

    val spark = SparkSession.builder
      .master("yarn")
      .appName("Partitioning...")
      .config(conf = ConfigObjDev.sparkConf)
      .getOrCreate()

    val sourcePath: String = args(0)
    val finalPath: String = args(1)
    val numPartReq = args(2).toInt
    val d = args(3).toInt
    val sc = spark.sparkContext
    val tmpDir = ".massiveRRP"
    val L1 = tmpDir + "/L1"
    val L1result = tmpDir + "/L1result"
    val L2 = tmpDir + "/L2"
    val L2result = tmpDir + "/L2result"

    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(hadoopConf)

    val numPart_d: Int = numPartReq / d

    randCutWithCount(sourcePath, L1, numPart_d)(hdfs)

    runRRP(L1, L1result)

    redistribute(L1result, L2)

    runRRP(L2, L2result)

    if (!hdfs.exists(new Path(finalPath))) hdfs.mkdirs(new Path(finalPath))
    moveFilesFromSubDir(L2result, finalPath)(hdfs)

    spark.stop()

    def runRRP(iPath: String, oPath: String): Unit = {
      val dirList = hdfs.listStatus(new Path(iPath))

      for (j <- 0 until dirList.length) {
        if (dirList(j).isDirectory) {
          val filePath = dirList(j).getPath
          val datasetRDD = sc.textFile(filePath.toString)
          RRP(datasetRDD).partitionByRRP(numPartReq).saveAsTextFile(oPath + "/" + filePath.getName)
        }
      }
    }

    def redistribute(iPath: String, oPath: String): Unit = {
      for (i <- 1 until d; o <- 1 until d) {
        takeRandomFiles(iPath + "/DS" + i, oPath + "/DS" + o, numPart_d / d, preserveFileName = false)(hdfs)
      }
    }
  }
}

