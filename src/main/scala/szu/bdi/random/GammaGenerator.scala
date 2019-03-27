package szu.bdi.random

import org.apache.commons.math3.distribution.GammaDistribution


/**
  * :: DeveloperApi ::
  * Generates i.i.d. samples from the gamma distribution with the given shape and scale.
  *
  * @param shape shape for the gamma distribution.
  * @param scale scale for the gamma distribution
  */
class GammaGenerator (
                       val shape: Double,
                       val scale: Double) extends RandomDataGenerator[Double] {

  private val rng = new GammaDistribution(shape, scale)

  override def nextValue(): Double = rng.sample()

  override def setSeed(seed: Long) {
    rng.reseedRandomGenerator(seed)
  }

  override def copy(): GammaGenerator = new GammaGenerator(shape, scale)
}

