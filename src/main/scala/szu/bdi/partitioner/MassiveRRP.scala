package szu.bdi.partitioner

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.reflect.ClassTag
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import szu.bdi.hdfs.Operations.{moveFilesFromSubDir, randCutWithCount, takeRandomFiles}
import szu.bdi.utils.HdfsIterator

class MassiveRRP(sourcePath: String, finalPath: String, Q: Int, d: Int)(implicit sc: SparkContext, hdfs: FileSystem)
  extends Logging with Serializable {

  private var tmpDir = ".massiveRRP"
  private def L1: String = tmpDir + "/L1"
  private def L1result: String = tmpDir + "/L1result"
  private def L2: String = tmpDir + "/L2"
  private def L2result: String = tmpDir + "/L2result"

  private def numPart_d: Int = (getP / d).toInt

  //private val hadoopConf = new Configuration()
  //private val hdfs: FileSystem = FileSystem.get(hadoopConf)

  def getP: Long ={
    val ri = hdfs.listFiles(new Path(sourcePath),false)
    HdfsIterator.getSize(ri)
  }

  def setTempPath(tmp: String): this.type = {
    this.tmpDir = tmp
    this
  }

  def getTempPath: String = tmpDir

  def getSourcePath: String = sourcePath

  def getFinalPath: String = finalPath

  def getQ: Int = Q

  def getd: Int = d

  def runMultiRRP(iPath: String, oPath: String): Unit = {
    val dirList = hdfs.listStatus(new Path(iPath))

    for (j <- 0 until dirList.length) {
      if (dirList(j).isDirectory) {
        val filePath = dirList(j).getPath
        val datasetRDD = sc.textFile(filePath.toString)
        RRP(datasetRDD).partitionByRRP(Q).saveAsTextFile(oPath + "/" + filePath.getName)
      }
    }
  }

  def redistribute(iPath: String, oPath: String): Unit = {
    for (i <- 1 until d; o <- 1 until d) {
      takeRandomFiles(iPath + "/DS" + i, oPath + "/DS" + o, numPart_d / d, preserveFileName = false)(hdfs)
    }
  }

  def run():Unit={
    randCutWithCount(sourcePath, L1, numPart_d)(hdfs)
    runMultiRRP(L1, L1result)
    redistribute(L1result, L2)
    runMultiRRP(L2, L2result)

    if (!hdfs.exists(new Path(finalPath))) hdfs.mkdirs(new Path(finalPath))
    moveFilesFromSubDir(L2result, finalPath)(hdfs)
  }
}

object MassiveRRP{
  def apply(sourcePath: String, finalPath: String, Q: Int, d: Int)(implicit sc: SparkContext, hdfs: FileSystem) = new MassiveRRP(sourcePath, finalPath, Q, d)
}