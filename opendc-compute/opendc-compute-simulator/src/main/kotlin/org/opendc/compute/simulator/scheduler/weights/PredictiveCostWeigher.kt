package org.opendc.compute.simulator.scheduler.weights

import org.opendc.compute.simulator.service.HostView
import org.opendc.compute.simulator.service.ServiceTask

/**
 * A weigher that predicts future costs based on task duration and current system state.
 */
public class PredictiveCostWeigher(override val multiplier: Double = 1.0) : HostWeigher {

    /**
     * Calculate the weight for a single host.
     */
    override fun getWeight(host: HostView, task: ServiceTask): Double {
        val currentCost = host.host.getCurrentCost()
        val cpuStats = host.host.getCpuStats()
        val systemStats = host.host.getSystemStats()

        // Basic cost factors
        val utilizationFactor = 1.0 + cpuStats.utilization  // Higher utilization might lead to higher future costs
        val memoryFactor = 1.0 + (1.0 - (host.availableMemory.toDouble() / host.host.getModel().memoryCapacity))
        val vmCountFactor = 1.0 + (systemStats.guestsRunning * 0.1)  // More VMs might indicate future resource contention

        // Predicted cost calculation
        val predictedCost = currentCost * utilizationFactor * memoryFactor * vmCountFactor

        System.out.println("""
            Prediction for host ${host.host.getName()}:
            - Current Cost: $currentCost
            - Utilization Factor: $utilizationFactor
            - Memory Factor: $memoryFactor
            - VM Count Factor: $vmCountFactor
            - Predicted Cost: $predictedCost
        """.trimIndent())

        return predictedCost
    }

    override fun toString(): String = "PredictiveCostWeigher(multiplier=$multiplier)"
}
