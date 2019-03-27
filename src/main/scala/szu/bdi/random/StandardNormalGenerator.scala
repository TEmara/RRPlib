package szu.bdi.random


/**
  * :: DeveloperApi ::
  * Generates i.i.d. samples from the standard normal distribution.
  */
class StandardNormalGenerator extends RandomDataGenerator[Double] {

  // XORShiftRandom for better performance. Thread safety isn't necessary here.
  private val random = new XORShiftRandom()

  override def nextValue(): Double = {
    random.nextGaussian()
  }

  override def setSeed(seed: Long): Unit = random.setSeed(seed)

  override def copy(): StandardNormalGenerator = new StandardNormalGenerator()
}
