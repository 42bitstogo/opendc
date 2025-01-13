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

package org.opendc.compute.simulator.scheduler.weights

import org.opendc.compute.simulator.service.HostView
import org.opendc.compute.simulator.service.ServiceTask
import java.time.Instant
import java.time.InstantSource
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/* Return the cost as the weight.
     The FilterScheduler picks the host with the lowest weight by default if no multiplier is negative.
     we want the lowest cost to win, so the multiplier is
     positive and the scheduler picks by minimum weight.*/
public class CostWeigher(override val multiplier: Double = 1.0) : HostWeigher {
    override fun getWeight(host: HostView, task: ServiceTask): Double {
        return host.host.getCurrentCost()

//        System.out.printf("CostWeigher: Host %s weight calculation - Cost: %.2f, Multiplier: %.2f%n",
//            host.host.getName(),
//            cost,
//            multiplier)
//
//        return cost * multiplier
    }

    override fun toString(): String = "CostWeigher(multiplier=$multiplier)"
}
