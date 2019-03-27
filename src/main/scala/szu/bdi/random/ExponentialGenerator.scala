package szu.bdi.random

import org.apache.commons.math3.distribution.ExponentialDistribution


/**
  * :: DeveloperApi ::
  * Generates i.i.d. samples from the exponential distribution with the given mean.
  *
  * @param mean mean for the exponential distribution.
  */
class ExponentialGenerator (
                             val mean: Double) extends RandomDataGenerator[Double] {

  private val rng = new ExponentialDistribution(mean)

  override def nextValue(): Double = rng.sample()

  override def setSeed(seed: Long) {
    rng.reseedRandomGenerator(seed)
  }

  override def copy(): ExponentialGenerator = new ExponentialGenerator(mean)
}

