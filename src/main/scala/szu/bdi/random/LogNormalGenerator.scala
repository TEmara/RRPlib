package szu.bdi.random

import org.apache.commons.math3.distribution.LogNormalDistribution


/**
  * :: DeveloperApi ::
  * Generates i.i.d. samples from the log normal distribution with the
  * given mean and standard deviation.
  *
  * @param mean mean for the log normal distribution.
  * @param std standard deviation for the log normal distribution
  */
class LogNormalGenerator (
                           val mean: Double,
                           val std: Double) extends RandomDataGenerator[Double] {

  private val rng = new LogNormalDistribution(mean, std)

  override def nextValue(): Double = rng.sample()

  override def setSeed(seed: Long) {
    rng.reseedRandomGenerator(seed)
  }

  override def copy(): LogNormalGenerator = new LogNormalGenerator(mean, std)
}

