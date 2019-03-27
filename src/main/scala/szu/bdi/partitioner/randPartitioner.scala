package szu.bdi.partitioner

import org.apache.spark.Partitioner

class randPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = {
    //val k = key.asInstanceOf[Int]
    val k=key.hashCode()
    k % numPartitions
    //val RecordPerPartition =861742
    //return k * numPartitions / RecordPerPartition
  }
}

