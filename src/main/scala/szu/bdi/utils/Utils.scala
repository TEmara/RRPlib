package szu.bdi.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Random}

object Utils {

  val random = new Random()

  def getTimestamp(x:Any) :java.sql.Timestamp = {
    val format = new SimpleDateFormat("MM-dd-yyyy' 'HH:mm:ss")
    if (x.toString == "")
      null
    else {
      val d = format.parse(x.toString)
      val t = new Timestamp(d.getTime)
      t
    }
  }

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0) + "ms")
    result
  }

  def getExecutionTime(block: => Unit): Long = {
    val start = System.currentTimeMillis
    block    // call-by-name
    System.currentTimeMillis - start
  }

  /**
    * Timing method based on iterations that permit JVM JIT optimization.
    *
    * @param numIters number of iterations
    * @param f function to be executed. If prepare is not None, the running time of each call to f
    *          must be an order of magnitude longer than one millisecond for accurate timing.
    * @param prepare function to be executed before each call to f. Its running time doesn't count.
    * @return the total time across all iterations (not counting preparation time)
    */
  def timeIt(numIters: Int)(f: => Unit, prepare: Option[() => Unit] = None): Long = {
    if (prepare.isEmpty) {
      val start = System.currentTimeMillis
      times(numIters)(f)
      System.currentTimeMillis - start
    } else {
      var i = 0
      var sum = 0L
      while (i < numIters) {
        prepare.get.apply()
        val start = System.currentTimeMillis
        f
        sum += System.currentTimeMillis - start
        i += 1
      }
      sum
    }
  }

  /**
    * Method executed for repeating a task for side effects.
    * Unlike a for comprehension, it permits JVM JIT optimization
    */
  def times(numIters: Int)(f: => Unit): Unit = {
    var i = 0
    while (i < numIters) {
      f
      i += 1
    }
  }

  /**
    * Counts the number of elements of an iterator using a while loop rather than calling
    * [[scala.collection.Iterator#size]] because it uses a for loop, which is slightly slower
    * in the current version of Scala.
    */
  def getIteratorSize[T](iterator: Iterator[T]): Long = {
    var count = 0L
    while (iterator.hasNext) {
      count += 1L
      iterator.next()
    }
    count
  }

  private val format = new SimpleDateFormat("yyyy.MM.dd HH.mm")
  def currentTime: String = format.format(System.currentTimeMillis())

  def onTheFives: Boolean = {
    val now = Calendar.getInstance().getTime
    val minuteFormat = new SimpleDateFormat("mm")
    val currentMinuteAsString = minuteFormat.format(now)
    try {
      val currentMinute = Integer.parseInt(currentMinuteAsString)
      if (currentMinute % 5 == 0) true
      else false
    } catch {
      case _: Throwable => false
    }
  }

  def longRange(a: Long, b: Long): Iterator[Long] = new Iterator[Long] {
    private[this] var i = a
    def hasNext: Boolean = i<b
    def next: Long = { val j = i; i += 1; j }
  }
}
