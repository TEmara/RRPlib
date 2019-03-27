package szu.bdi.rdd

import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import szu.bdi.random.RandomDataGenerator
import szu.bdi.utils.Utils

import scala.reflect.ClassTag
import scala.util.Random

private class MultivariateRDDPartition[T](override val index: Int,
                                    val size: Int,
                                    val generators: Array[RandomDataGenerator[T]],
                                    val seed: Long) extends Partition {

  require(size >= 0, "Non-negative partition size required.")
}

class MultivariateRDD(sc: SparkContext,
                      size: Long,
                      vectorSize: Int,
                      numPartitions: Int,
                      @transient private val rngArr: Array[RandomDataGenerator[Double]],
                      @transient private val seed: Long = Utils.random.nextLong) extends RDD[Vector](sc, Nil) {

  require(size > 0, "Positive RDD size required.")
  require(numPartitions > 0, "Positive number of partitions required")
  require(vectorSize > 0, "Positive vector size required.")
  require(math.ceil(size.toDouble / numPartitions) <= Int.MaxValue,
    "Partition size cannot exceed Int.MaxValue")

  override def compute(splitIn: Partition, context: TaskContext): Iterator[Vector] = {
    val split = splitIn.asInstanceOf[MultivariateRDDPartition[Double]]
    MultivariateRDD.getVectorIterator(split, vectorSize)
  }

  override protected def getPartitions: Array[Partition] = {
    MultivariateRDD.getPartitions(size, numPartitions, rngArr, seed)
  }
}

private object MultivariateRDD {

  def getPartitions[T](size: Long,
                       numPartitions: Int,
                       rngArr: Array[RandomDataGenerator[T]],
                       seed: Long): Array[Partition] = {

    val partitions = new Array[MultivariateRDDPartition[T]](numPartitions)
    var i = 0
    var start: Long = 0
    var end: Long = 0
    val random = new Random(seed)
    while (i < numPartitions) {
      end = ((i + 1) * size) / numPartitions
      partitions(i) = new MultivariateRDDPartition(i, (end - start).toInt, rngArr, random.nextLong())
      start = end
      i += 1
    }
    partitions.asInstanceOf[Array[Partition]]
  }

  // The RNG has to be reset every time the iterator is requested to guarantee same data
  // every time the content of the RDD is examined.
  def getPointIterator[T: ClassTag](partition: RandomRDDPartition[T]): Iterator[T] = {
    val generator = partition.generator.copy()
    generator.setSeed(partition.seed)
    Iterator.fill(partition.size)(generator.nextValue())
  }

  // The RNG has to be reset every time the iterator is requested to guarantee same data
  // every time the content of the RDD is examined.
  def getVectorIterator(
                         partition: MultivariateRDDPartition[Double],
                         vectorSize: Int): Iterator[Vector] = {
    val generators = new Array[RandomDataGenerator[Double]](vectorSize)
    for (i <- 0 until vectorSize){
      generators(i) = partition.generators(i).copy()
      generators(i).setSeed(partition.seed)
    }

    def nextRecord: Array[Double]={
      val res = new Array[Double](vectorSize)
      for (i <- 0 until vectorSize){
        res(i)= generators(i).nextValue()
      }
      res
    }
    Iterator.fill(partition.size)(new DenseVector(nextRecord))
  }
}
