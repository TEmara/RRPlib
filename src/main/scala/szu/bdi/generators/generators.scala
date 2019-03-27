package szu.bdi.generators

object generators{
  val integers: Generator[Int] = new Generator[Int] {
    def generate: Int = scala.util.Random.nextInt()
  }
  /*
  val booleans = new Generator[Boolean] {
    def generate = integers.generate > 0
  }
  // Or
  val booleans = for (x <- integers) yield x > 0

  val pairs = new Generator[(Int, Int)] {
    def generate = (integers.generate, integers.generate)
  }
  */
  // simple ways to implement booleans and pairs
  val booleans: Generator[Boolean] = integers.map(_ >= 0)

  def pairs[T, U](t: Generator[T], u: Generator[U]): Generator[(T, U)] = for {
    x <- t
    y <- u
  } yield (x, y)

  def single[T](x: T): Generator[T] = new Generator[T] {
    def generate: T = x
  }

  def choose(lo: Int, hi: Int): Generator[Int] =
    for (x <- integers) yield lo + x % (hi - lo)

  def oneOf[T](xs: T*): Generator[T] =
    for (idx <- choose(0, xs.length)) yield xs(idx)

  def lists: Generator[List[Int]] = for {
    isEmpty <- booleans
    list <- if (isEmpty) emptyLists else nonEmptyLists
  } yield list

  def emptyLists = single(Nil)

  def nonEmptyLists = for {
    head <- integers
    tail <- lists
  } yield head :: tail

  def test[T](g: Generator[T], numTimes: Int = 100)
             (test: T => Boolean): Unit = {
    for (i <- 0 until numTimes) {
      val value = g.generate
      assert(test(value), "test failed for "+value)
    }
    println("passed "+numTimes+ " tests")
  }
}