package szu.bdi.random

import org.apache.commons.math3.distribution.WeibullDistribution


/**
  * :: DeveloperApi ::
  * Generates i.i.d. samples from the Weibull distribution with the
  * given shape and scale parameter.
  *
  * @param alpha shape parameter for the Weibull distribution.
  * @param beta scale parameter for the Weibull distribution.
  */
class WeibullGenerator(
                        val alpha: Double,
                        val beta: Double) extends RandomDataGenerator[Double] {

  private val rng = new WeibullDistribution(alpha, beta)

  override def nextValue(): Double = rng.sample()

  override def setSeed(seed: Long): Unit = {
    rng.reseedRandomGenerator(seed)
  }

  override def copy(): WeibullGenerator = new WeibullGenerator(alpha, beta)
}

