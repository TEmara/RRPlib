package szu.bdi.partitioner

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class RRP[T: ClassTag](self: RDD[T])//(implicit tt: ClassTag[T])
  extends Logging with Serializable {

  def partitionByRRP(numParts: Int) : RDD[T]  = {

    def addingRandomKey(index: Int, iter: Iterator[T]) : Iterator[(Int, T)]= {
      val itx = iter.toList
      val numRecords = itx.length
      val arrIndex=(0 until numRecords).toList
      val arrIter = scala.util.Random.shuffle(arrIndex).toIterator
      arrIter.zip(itx.iterator)
    }

    self.mapPartitionsWithIndex(addingRandomKey).partitionBy(new randPartitioner(numParts)).values
  }
}

object RRP{
  def apply[T: ClassTag](self: => RDD[T]) = new RRP(self)
}



