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

package org.opendc.org.opendc.compute.price

import org.opendc.simulator.compute.models.CostDto
import org.opendc.simulator.compute.power.CarbonFragment
import org.opendc.trace.Trace
import org.opendc.trace.conv.COST_TIMESTAMP
import org.opendc.trace.conv.COST_VALUE
import org.opendc.trace.conv.TABLE_CARBON_INTENSITIES
import java.io.File
import java.time.Instant

public class CostTraceLoader() {
//    fun getCostTrace(): CostTraceReader {
//        val costData = loadCostDataFromFile(costTracePath)
//        return SimpleCostTraceReader(costData)
//    }

    public fun get(pathToFile: File): List<CostDto> {
        val trace = Trace.open(pathToFile, "cost")
        return parseCost(trace)
    }

    private fun parseCost(trace: Trace): List<CostDto> {
        val reader = checkNotNull(trace.getTable(TABLE_CARBON_INTENSITIES).newReader())

        val costs: MutableList<CostDto> = mutableListOf()
        val costTimestamp = reader.resolve(COST_TIMESTAMP)
        val costColumn = reader.resolve(COST_VALUE)

        try {
            while (reader.nextRow()) {
                val timestamp = reader.getInstant(costTimestamp)!!
                val cost = reader.getDouble(costColumn)

                costs.add(CostDto(timestamp, cost))
            }
            return costs
        } catch (e: Exception) {
            e.printStackTrace()
            throw e
        } finally {
            reader.close()
        }
    }

//    private fun loadCostDataFromFile(path: String): List<CostModel> {
//        // Implement logic to read from Parquet file
//        // For demonstration, let's assume it's a JSON file
//        val file = File(path)
//        // Parse the file and build the costData map
//        // Map<HostId, List<CostDto>>
//        // adit-TODO: Implement actual file reading and parsing
//        return emptyMap()
//    }
}

// SimpleCostTraceReader.kt
class SimpleCostTraceReader(private val costData: Map<String, List<CostDto>>) : CostTraceReader {
    override fun getCost(
        hostId: String,
        timestamp: Long,
    ): Double? {
        val costList = costData[hostId] ?: return null
        val costDto = costList.lastOrNull { it.timestamp <= timestamp }
        return costDto?.cost
    }

    override fun getNextCostUpdate(
        hostId: String,
        timestamp: Long,
    ): Long? {
        val costList = costData[hostId] ?: return null
        val nextCostDto = costList.firstOrNull { it.timestamp > timestamp }
        return nextCostDto?.timestamp
    }
}

/**
 * A builder for a VM trace.
 */
private class CostFragmentBuilder {
    /**
     * The total load of the trace.
     */
    public val fragments: MutableList<CarbonFragment> = mutableListOf()

    /**
     * Add a fragment to the trace.
     *
     * @param startTime Timestamp at which the fragment starts (in epoch millis).
     * @param carbonIntensity The carbon intensity during this fragment
     */
    fun add(
        startTime: Instant,
        carbonIntensity: Double,
    ) {
        fragments.add(
            CarbonFragment(
                startTime.toEpochMilli(),
                Long.MAX_VALUE,
                carbonIntensity,
            ),
        )
    }

    fun fixReportTimes() {
        fragments.sortBy { it.startTime }

        // For each report, set the end time to the start time of the next report
        for (i in 0..fragments.size - 2) {
            fragments[i].endTime = fragments[i + 1].startTime
        }

        // Set the start time of each report to the minimum value
        fragments[0].startTime = Long.MIN_VALUE
    }
}
