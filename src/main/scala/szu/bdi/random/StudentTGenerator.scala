package szu.bdi.random

import org.apache.commons.math3.distribution.TDistribution


/**
  * Developed by Tamer Emara
  * On April 1, 2018
  *
  * :: DeveloperApi ::
  * Generates i.i.d. samples from the student's t-distribution with the
  * given degrees of freedom.
  *
  * @param degreesOfFreedom degrees of freedom for the student's t-distribution.
  */
class StudentTGenerator(
                         val degreesOfFreedom: Double) extends RandomDataGenerator[Double] {

  private val rng = new TDistribution(degreesOfFreedom)

  override def nextValue(): Double = rng.sample()

  override def setSeed(seed: Long): Unit = {
    rng.reseedRandomGenerator(seed)
  }

  override def copy(): StudentTGenerator = new StudentTGenerator(degreesOfFreedom)
}

