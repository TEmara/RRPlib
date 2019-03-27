package szu.bdi.generators

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.random.RandomRDDs
import java.util.concurrent.{ExecutorService, Executors}

import scala.util.Random
import org.apache.spark.{SparkConf, SparkContext}
import szu.bdi.utils.Utils

object GenerateDataSet {
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
      //      println("<Q>: Number of partitions for the output dataset.")
      System.exit(1)
    }
    val conf: SparkConf = new SparkConf()
      .setAppName("Generate_DataSet " + args(0))
      .setMaster("yarn")
      .set("spark.submit.deployMode","client")
      //.setMaster("yarn-client")
      .set("spark.executor.memory", "25g")// 10
      .set("spark.executor.cores", "16")// 5
      .set("spark.executor.instances", "43")//20 149
    //.set("spark.yarn.executor.memoryOverhead","1024")
    //.set("spark.driver.memory", "10g")
    //.set("spark.driver.cores", "5")
    //.set("spark.yarn.driver.memoryOverhead","1024")
    //.set("spark.scheduler.listenerbus.eventqueue.size","100000")
    //.set("spark.default.parallelism","1490")

    val sc = new SparkContext(conf)
    val FinalPath: String = args(0)
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(hadoopConf)
    val ClassCount: Int = args(3).toInt // K
    val recordCount: Long = args(1).toLong / ClassCount // R/K
    //real total record no. = recordCount * ClassCount * repeatFactor
    val repeatFactor: Int = if (args.length > 7) args(7).toInt else 1
    val featureCount: Int = args(2).toInt
    val shiftFactor = if (args.length > 5) args(5).toInt else 0
    val noiseFeature = if (args.length > 6) args(6).toInt else 0
    val numPartitions: Int = if((args(4).toInt / ClassCount)>1) args(4).toInt / ClassCount else 1// real total number of partitions = numPartitions * ClassCount * repeatFactor
    //val FinalPath: String = "NDS003"

    //    GenerateRegressionData(sc, featureCount, repeatFactor, recordCount, numPartitions, FinalPath)

    hdfs.mkdirs(new Path(FinalPath))
    /*
    val T:Array[Thread] = new Array[Thread](ClassCount)
    for (i <- 0 until ClassCount) {
      T(i)= new Thread(new Runnable with Serializable {
        override def run(): Unit = {
          GenerateData(sc, featureCount, repeatFactor, recordCount, numPartitions, i + shiftFactor, noiseFeature)
          MoveDataFiles(hdfs, FinalPath, i + shiftFactor)
        }
      })
      //Thread.sleep(5000)
      T(i).start()
    }
    */
    val threads =
      for (i <- (1 to ClassCount).toList)
        yield new Thread(new Runnable with Serializable {
          override def run(): Unit = {
            GenerateData(sc, featureCount, repeatFactor, recordCount, numPartitions, i + shiftFactor, noiseFeature)
            MoveDataFiles(hdfs, FinalPath, i + shiftFactor)
          }
        })
    val start = System.currentTimeMillis()

    threads.foreach(t => t.start())
    threads.foreach(t => t.join())

    val processingTime = System.currentTimeMillis - start
    val resPath = "res/generating"
    sc.parallelize(Seq(recordCount, featureCount, ClassCount, numPartitions, processingTime),1).saveAsTextFile(resPath + "/" + Utils.currentTime)
    println("all done")
  }

  def GenerateData( sc :SparkContext,
                    featureCount :Int,
                    repeatFactor :Int,
                    recordCount :Long,
                    numPartitions :Int,
                    iClassCount:Int,
                    noiseFeature: Int): Unit = {
    //val y:Array[org.apache.spark.rdd.RDD[scala.collection.immutable.Vector[Any]]]=new Array[org.apache.spark.rdd.RDD[scala.collection.immutable.Vector[Any]]](repeatFactor)
    //var UnRDD :org.apache.spark.rdd.RDD[scala.collection.immutable.Vector[Any]]=sc.emptyRDD[scala.collection.immutable.Vector[Any]]
    var UnRDD :org.apache.spark.rdd.RDD[Array[Double]]=sc.emptyRDD
    //val w = Array.fill(featureCount)(random.nextDouble() - 0.5)

    val means :Vector[Int]= Vector.fill(featureCount)(new Random(Random.nextLong).nextInt(10))
    val sigmas :Vector[Double]=Vector.fill(featureCount)(new Random(Random.nextLong).nextInt(10))

    for(rF <- 0 until repeatFactor){
      val y = RandomRDDs.normalVectorRDD(sc, recordCount, featureCount-noiseFeature, numPartitions).map {v =>
        //var res: Vector[Double] = scala.collection.immutable.Vector.empty
        var res = new Array[Double](0)
        for (i <- 0 until featureCount-noiseFeature){
          def x = means(i) + sigmas(i) * v(i)
          res = res:+ BigDecimal(x).setScale(7, BigDecimal.RoundingMode.HALF_UP).toDouble
        }
        val random = new Random(Random.nextLong)
        for (i <- featureCount-noiseFeature until featureCount){
          //res = res:+ random.nextDouble()*random.nextInt(10)
          res = res:+ random.nextInt(100)*1.0
        }
        res = res:+ iClassCount.toDouble //((65+i).toChar.toString)
        res
      }
      UnRDD = UnRDD ++ y
    }

    //    UnRDD.map{x=>
    //      var res :String = x(0).toString
    //      for (i <- 1 to featureCount){
    //        res = res + " , " + x(i)
    //      }
    //      res
    //    }.saveAsTextFile("/user/tamer/temp_first/DS" + iClassCount)
    UnRDD.map(x=> x.mkString(",")).saveAsTextFile("tmp/DS" + iClassCount)
  }
  def MoveDataFiles( hdfs :FileSystem,
                     DestPath :String,
                     ClassCount:Int): Unit = {
    if (ClassCount <0){
      val fileListDir = hdfs.listStatus(new Path("tmp/RDS"))
      val df = new java.text.DecimalFormat("000000")
      for (i <- 1 until  fileListDir.length){
        val ok = hdfs.rename(fileListDir(i).getPath, new Path(DestPath + "/part" + ClassCount + "-" + df.format(i)))
      }
      println("Generating dataset is done")
      println("Delete tmp/RDS: "+ hdfs.delete(new Path("tmp/RDS"),true))
    }else{
      val fileListDir = hdfs.listStatus(new Path("tmp/DS" + ClassCount))
      val df = new java.text.DecimalFormat("000000")
      for (i <- 1 until fileListDir.length){
        val ok = hdfs.rename(fileListDir(i).getPath, new Path(DestPath + "/part" + ClassCount + "-" + df.format(i)))
      }
      println("Generating Class " + ClassCount +" done")
      println("Delete tmp/DS" + ClassCount +" : "+ hdfs.delete(new Path("tmp/DS" + ClassCount),true))
    }
  }

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
}
