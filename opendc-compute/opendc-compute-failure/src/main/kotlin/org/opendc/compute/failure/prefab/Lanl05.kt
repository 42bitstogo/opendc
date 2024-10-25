/*
 * Copyright (c) 2024 AtLarge Research
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.opendc.compute.failure.prefab

import org.apache.commons.math3.distribution.ExponentialDistribution
import org.apache.commons.math3.distribution.GammaDistribution
import org.apache.commons.math3.distribution.LogNormalDistribution
import org.apache.commons.math3.distribution.UniformRealDistribution
import org.apache.commons.math3.distribution.WeibullDistribution
import org.apache.commons.math3.random.Well19937c
import org.opendc.compute.failure.models.SampleBasedFailureModel
import org.opendc.compute.simulator.service.ComputeService
import java.time.InstantSource
import java.util.random.RandomGenerator
import kotlin.coroutines.CoroutineContext

/**
 * Failure models based on values taken from "The Failure Trace Archive: Enabling the comparison of failure measurements and models of distributed systems"
 * Which can be found at https://www-sciencedirect-com.vu-nl.idm.oclc.org/science/article/pii/S0743731513000634
 */
public fun createLanl05Exp(
    context: CoroutineContext,
    clock: InstantSource,
    service: ComputeService,
    random: RandomGenerator,
): SampleBasedFailureModel {
    val rng = Well19937c(random.nextLong())

    return SampleBasedFailureModel(
        context,
        clock,
        service,
        random,
        ExponentialDistribution(rng, 1779.99),
        ExponentialDistribution(rng, 5.92),
        UniformRealDistribution(0.0, 1.0),
    )
}

public fun createLanl05Wbl(
    context: CoroutineContext,
    clock: InstantSource,
    service: ComputeService,
    random: RandomGenerator,
): SampleBasedFailureModel {
    val rng = Well19937c(random.nextLong())

    return SampleBasedFailureModel(
        context,
        clock,
        service,
        random,
        WeibullDistribution(rng, 0.48, 816.60),
        WeibullDistribution(rng, 0.58, 2.18),
        UniformRealDistribution(0.0, 1.0),
    )
}

public fun createLanl05LogN(
    context: CoroutineContext,
    clock: InstantSource,
    service: ComputeService,
    random: RandomGenerator,
): SampleBasedFailureModel {
    val rng = Well19937c(random.nextLong())

    return SampleBasedFailureModel(
        context,
        clock,
        service,
        random,
        LogNormalDistribution(rng, 5.56, 2.39),
        LogNormalDistribution(rng, 0.05, 1.42),
        UniformRealDistribution(0.0, 1.0),
    )
}

public fun createLanl05Gam(
    context: CoroutineContext,
    clock: InstantSource,
    service: ComputeService,
    random: RandomGenerator,
): SampleBasedFailureModel {
    val rng = Well19937c(random.nextLong())

    return SampleBasedFailureModel(
        context,
        clock,
        service,
        random,
        GammaDistribution(rng, 0.35, 5102.71),
        GammaDistribution(rng, 0.38, 15.44),
        UniformRealDistribution(0.0, 1.0),
    )
}
