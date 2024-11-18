package org.opendc.org.opendc.compute.price

import java.io.File
import java.lang.ref.SoftReference
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

public class CostTraceLoader {
    /**
     * The cache of workloads.
     */
    private val cache = ConcurrentHashMap<String, SoftReference<List<CarbonFragment>>>()

    private val builder = CarbonFragmentNewBuilder()

    /**
     * Read the metadata into a workload.
     */
    private fun parseCarbon(trace: Trace): List<CarbonFragment> {
        val reader = checkNotNull(trace.getTable(TABLE_CARBON_INTENSITIES)).newReader()

        val startTimeCol = reader.resolve(CARBON_INTENSITY_TIMESTAMP)
        val carbonIntensityCol = reader.resolve(CARBON_INTENSITY_VALUE)

        try {
            while (reader.nextRow()) {
                val startTime = reader.getInstant(startTimeCol)!!
                val carbonIntensity = reader.getDouble(carbonIntensityCol)

                builder.add(startTime, carbonIntensity)
            }

            // Make sure the virtual machines are ordered by start time
            builder.fixReportTimes()

            return builder.fragments
        } catch (e: Exception) {
            e.printStackTrace()
            throw e
        } finally {
            reader.close()
        }
    }

    /**
     * Load the trace with the specified [name] and [format].
     */
    public fun get(pathToFile: File): List<CarbonFragment> {
        val trace = Trace.open(pathToFile, "carbon")

        return parseCarbon(trace)
    }

    /**
     * Clear the workload cache.
     */
    public fun reset() {
        cache.clear()
    }

    /**
     * A builder for a VM trace.
     */
    private class CarbonFragmentNewBuilder {
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
}
