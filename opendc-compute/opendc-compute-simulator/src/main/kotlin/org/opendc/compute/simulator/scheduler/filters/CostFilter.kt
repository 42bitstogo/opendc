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

package org.opendc.compute.simulator.scheduler.filters

import org.opendc.compute.simulator.service.HostView
import org.opendc.compute.simulator.service.ServiceTask
import java.time.Instant
import java.time.InstantSource
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

public class CostFilter(private val maxCost: Double = 1000.0) : HostFilter {
    override fun test(host: HostView, task: ServiceTask): Boolean {
        val currentCost = host.host.getCurrentCost()

        System.out.printf("CostFilter: Host %s cost %.2f at %s%n",
            host.host.getName(),
            currentCost,
            host.host.getSystemStats())

        return currentCost <= maxCost
    }
}
