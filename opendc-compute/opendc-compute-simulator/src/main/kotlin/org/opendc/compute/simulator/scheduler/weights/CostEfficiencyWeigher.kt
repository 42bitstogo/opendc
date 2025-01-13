package org.opendc.compute.simulator.scheduler.weights

import org.opendc.compute.simulator.service.HostView
import org.opendc.compute.simulator.service.ServiceTask
import kotlin.math.max

/**
 * A weigher that calculates a composite score based on cost efficiency.
 * Lower score is better (will be chosen by scheduler).
 */
public class CostEfficiencyWeigher(override val multiplier: Double = 1.0) : HostWeigher {
    override fun getWeight(host: HostView, task: ServiceTask): Double {
        val stats = host.host.getSystemStats()
        val cpuStats = host.host.getCpuStats()

        // Get current cost
        val cost = host.host.getCurrentCost()

        // Calculate CPU utilization (as a percentage)
        val cpuUtilization = cpuStats.utilization * 100

        // Calculate number of running VMs
        val runningVms = stats.guestsRunning

        // Calculate a composite score:
        // - Higher cost increases the score (worse)
        // - Higher CPU utilization slightly increases the score (we want some headroom)
        // - More running VMs slightly increases the score (prefer load distribution)
        val costFactor = cost / 100.0  // Normalize cost
        val utilizationFactor = cpuUtilization / 50.0  // Prefer hosts with less than 50% utilization
        val vmFactor = runningVms / 5.0  // Normalize by expecting ~5 VMs per host

        // Composite score calculation - lower is better
        val score = (costFactor * 0.6) +  // Cost is the primary factor (60% weight)
            (utilizationFactor * 0.3) +  // Utilization is secondary (30% weight)
            (vmFactor * 0.1)  // Number of VMs has small impact (10% weight)

        // Ensure we never return a negative weight
        return max(score, 0.0) * multiplier
    }

    override fun toString(): String = "CostEfficiencyWeigher(multiplier=$multiplier)"
}
