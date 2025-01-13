package org.opendc.compute.simulator.scheduler

import org.opendc.compute.simulator.scheduler.filters.ComputeFilter
import org.opendc.compute.simulator.scheduler.filters.RamFilter
import org.opendc.compute.simulator.scheduler.filters.VCpuFilter
import org.opendc.compute.simulator.scheduler.filters.CostFilter
import org.opendc.compute.simulator.service.HostView
import org.opendc.compute.simulator.service.ServiceTask
import kotlin.math.exp

/**
 * A sophisticated scheduler that predicts future costs and resource usage patterns.
 * Uses exponential smoothing for predictions and considers multiple factors.
 */
public class PredictiveCostScheduler : ComputeScheduler {
    private val hosts = mutableSetOf<HostView>()
    private val historicalLoads = mutableMapOf<String, MutableList<Double>>()
    private val historicalCosts = mutableMapOf<String, MutableList<Double>>()

    // Configurable parameters
    private val alpha = 0.3 // Smoothing factor for exponential smoothing
    private val costWeight = 0.4
    private val utilizationWeight = 0.3
    private val balancingWeight = 0.3

    // Initialize filters with more lenient ratios for better distribution
    private val filters = mutableListOf(
        ComputeFilter(),
        VCpuFilter(0.8), // More lenient CPU ratio
        RamFilter(1.2),  // More lenient RAM ratio
        CostFilter(maxCost = 1500.0)  // Higher cost ceiling for more options
    )

    override fun addHost(host: HostView) {
        hosts.add(host)
        historicalLoads[host.host.getName()] = mutableListOf()
        historicalCosts[host.host.getName()] = mutableListOf()
        println("PredictiveCostScheduler: Added host ${host.host.getName()} to monitoring")
    }

    override fun removeHost(host: HostView) {
        hosts.remove(host)
        historicalLoads.remove(host.host.getName())
        historicalCosts.remove(host.host.getName())
        println("PredictiveCostScheduler: Removed host ${host.host.getName()} from monitoring")
    }

    private fun predictFutureCost(host: HostView): Double {
        val hostName = host.host.getName()
        val currentCost = host.host.getCurrentCost()
        val costs = historicalCosts.getOrPut(hostName) { mutableListOf() }
        costs.add(currentCost)

        // Keep only recent history
        if (costs.size > 10) costs.removeAt(0)

        // Calculate trend
        if (costs.size >= 2) {
            val trend = (costs.last() - costs.first()) / costs.size
            return currentCost * (1 + trend)
        }
        return currentCost
    }

    private fun calculateLoadScore(host: HostView): Double {
        val cpuStats = host.host.getCpuStats()
        val systemStats = host.host.getSystemStats()

        // Update historical load
        val currentLoad = cpuStats.utilization
        val loads = historicalLoads.getOrPut(host.host.getName()) { mutableListOf() }
        loads.add(currentLoad)
        if (loads.size > 10) loads.removeAt(0)

        // Calculate exponentially smoothed load
        val smoothedLoad = loads.foldIndexed(0.0) { index, acc, load ->
            acc + load * exp(-alpha * (loads.size - 1 - index))
        } / loads.size

        return smoothedLoad
    }

    private fun calculateHostScore(host: HostView, task: ServiceTask): Double {
        val predictedCost = predictFutureCost(host)
        val loadScore = calculateLoadScore(host)
        val balancingScore = host.host.getSystemStats().guestsRunning / hosts.size.toDouble()

        val costScore = 1.0 - (predictedCost / 1500.0) // Normalize cost
        val utilizationScore = 1.0 - loadScore // Prefer less loaded hosts
        val distributionScore = 1.0 - balancingScore // Prefer hosts with fewer VMs

        val finalScore = (costWeight * costScore +
            utilizationWeight * utilizationScore +
            balancingWeight * distributionScore)

        println("""
            PredictiveCostScheduler Scoring for ${host.host.getName()}:
            - Predicted Cost: $predictedCost (score: $costScore)
            - Load Score: $loadScore (score: $utilizationScore)
            - Distribution Score: $balancingScore (score: $distributionScore)
            - Final Score: $finalScore
        """.trimIndent())

        return finalScore
    }

    override fun select(task: ServiceTask): HostView? {
        println("\nPredictiveCostScheduler: Selecting host for task ${task.name}")

        // Filter hosts based on all criteria
        val eligibleHosts = hosts.filter { host ->
            filters.all { filter ->
                val passes = filter.test(host, task)
                if (!passes) {
                    println("Host ${host.host.getName()} failed filter: ${filter.javaClass.simpleName}")
                }
                passes
            }
        }

        if (eligibleHosts.isEmpty()) {
            println("No eligible hosts found for task ${task.name}")
            return null
        }

        // Select host with best predicted score
        return eligibleHosts.maxByOrNull { host ->
            calculateHostScore(host, task)
        }.also { selectedHost ->
            selectedHost?.let {
                println("""
                    Selected host ${it.host.getName()} for task ${task.name}
                    Current cost: ${it.host.getCurrentCost()}
                    CPU utilization: ${it.host.getCpuStats().utilization}
                    Running VMs: ${it.host.getSystemStats().guestsRunning}
                """.trimIndent())
            }
        }
    }
}
