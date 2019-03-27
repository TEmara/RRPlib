package szu.bdi.utils

import org.apache.hadoop.fs.RemoteIterator

// returns an iterator from a hadoop remote iterator
object HdfsIterator {
  def remote[T](iterator: RemoteIterator[T]): Iterator[T] = new Iterator[T] {
    override def hasNext(): Boolean = iterator.hasNext
    override def next(): T = iterator.next()
  }

  def getSize[T](iterator: RemoteIterator[T]): Long = {
    Utils.getIteratorSize(remote(iterator))
  }

}

