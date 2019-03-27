package szu.bdi.random

/**
  * :: DeveloperApi ::
  * Generates i.i.d. samples from U[0.0, 1.0]
  */

class UniformGenerator extends RandomDataGenerator[Double] {

  // XORShiftRandom for better performance. Thread safety isn't necessary here.
  private val random = new XORShiftRandom()

  override def nextValue(): Double = {
    random.nextDouble()
  }

  override def setSeed(seed: Long): Unit = random.setSeed(seed)

  override def copy(): UniformGenerator = new UniformGenerator()
}
