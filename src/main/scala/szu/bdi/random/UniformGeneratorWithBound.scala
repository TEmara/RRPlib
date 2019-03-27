package szu.bdi.random


/**
  * :: DeveloperApi ::
  * Generates i.i.d. samples from U[a, b]
  */

class UniformGeneratorWithBound (val a: Double,
                                 val b: Double
                                ) extends RandomDataGenerator[Double] {

  // XORShiftRandom for better performance. Thread safety isn't necessary here.
  private val random = new XORShiftRandom()

  override def nextValue(): Double = {
    //v => a + (b - a) * v
    a + (b - a) * random.nextDouble()
  }

  override def setSeed(seed: Long): Unit = random.setSeed(seed)

  override def copy(): UniformGenerator = new UniformGenerator()
}
