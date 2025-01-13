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

package org.opendc.compute.simulator.cost
import org.opendc.compute.simulator.models.CostDto
import org.opendc.trace.Trace
import org.opendc.trace.conv.COST
import org.opendc.trace.conv.COST_VALUE
import org.opendc.trace.conv.END_TIME
import org.opendc.trace.conv.START_TIME
import java.io.File
import java.time.Instant

public class PriceTraceLoader() {
    public fun get(pathToFile: File): List<CostDto> {
        val trace = Trace.open(pathToFile, COST)
        return parseCost(trace)
    }

    private fun parseCost(trace: Trace): List<CostDto> {
        val reader = checkNotNull(trace.getTable(COST)!!.newReader())

        val costs: MutableList<CostDto> = mutableListOf()
        val start = reader.resolve(START_TIME)
        val end = reader.resolve(END_TIME)
        val costColumn = reader.resolve(COST_VALUE)

        try {
            while (reader.nextRow()) {
                val startTime = reader.getInstant(start)!!
                val endTime = reader.getInstant(end)!!
                val cost = reader.getDouble(costColumn)
                costs.add(CostDto(startTime.toEpochMilli(), endTime.toEpochMilli(), cost))
            }
            return costs
        } catch (e: Exception) {
            e.printStackTrace()
            throw e
        } finally {
            reader.close()
        }
    }
}

/**
 * A builder for a VM trace.
 */
private class CostFragmentBuilder {
    /**
     * The total load of the trace.
     */
    public val fragments: MutableList<CostDto> = mutableListOf()

    /**
     * Add a fragment to the trace.
     *
     * @param startTime Timestamp at which the fragment starts (in epoch millis).
     * @param cost The cost during this fragment
     */
    fun add(
        startTime: Instant,
        endTime: Instant,
        cost: Double,
    ) {
        fragments.add(
            CostDto(
                startTime.toEpochMilli(),
                endTime.toEpochMilli(),
                cost
            ),
        )
    }
}
