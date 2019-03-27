package szu.bdi.random

import org.apache.commons.math3.distribution.PoissonDistribution


/**
  * :: DeveloperApi ::
  * Generates i.i.d. samples from the Poisson distribution with the given mean.
  *
  * @param mean mean for the Poisson distribution.
  */
class PoissonGenerator (
                         val mean: Double) extends RandomDataGenerator[Double] {

  private val rng = new PoissonDistribution(mean)

  override def nextValue(): Double = rng.sample()

  override def setSeed(seed: Long) {
    rng.reseedRandomGenerator(seed)
  }

  override def copy(): PoissonGenerator = new PoissonGenerator(mean)
}
