package szu.bdi.random


/**
  * :: DeveloperApi ::
  * Generates i.i.d. samples from the standard normal distribution.
  */
class NormalGenerator(
                       val mean: Double,
                       val std: Double) extends RandomDataGenerator[Double] {

  // XORShiftRandom for better performance. Thread safety isn't necessary here.
  private val random = new XORShiftRandom()

  override def nextValue(): Double = {
    //v => mean + sigma * v
    mean + std * random.nextGaussian()
  }

  override def setSeed(seed: Long): Unit = random.setSeed(seed)

  override def copy(): StandardNormalGenerator = new StandardNormalGenerator()
}
