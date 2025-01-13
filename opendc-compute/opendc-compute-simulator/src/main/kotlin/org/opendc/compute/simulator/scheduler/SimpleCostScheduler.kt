package org.opendc.compute.simulator.scheduler;


import mu.KotlinLogging
import org.opendc.compute.simulator.scheduler.filters.ComputeFilter
import org.opendc.compute.simulator.scheduler.filters.RamFilter
import org.opendc.compute.simulator.scheduler.filters.VCpuFilter
import org.opendc.compute.simulator.service.HostView
import org.opendc.compute.simulator.service.ServiceTask

/**
 * A simple scheduler that selects the host with the lowest current cost.
 *
 * If multiple hosts have the same cost, it selects the one with the most available memory
 * as a secondary criteria to help with load balancing.
 */
public class SimpleCostScheduler : ComputeScheduler {
    private val logger = KotlinLogging.logger {}

// Add to ComputeSchedulerEnum in ComputeSchedulers.kt:
// SimpleCost,

// Add to createComputeScheduler match in ComputeSchedulers.kt:
// ComputeSchedulerEnum.SimpleCost -> SimpleCostScheduler()

    /**
     * The pool of hosts available to the scheduler.
     */
    private val hosts = mutableListOf<HostView>()
    private val filters = mutableListOf(ComputeFilter(), VCpuFilter(1.0), RamFilter(1.5))

    override fun addHost(host: HostView) {
        hosts.add(host)
    }

    override fun removeHost(host: HostView) {
        hosts.remove(host)
    }

    override fun select(task: ServiceTask): HostView? {
        val hosts = hosts.filter { host -> filters.all { filter -> filter.test(host, task) } }
        if (hosts.isEmpty()) {
            logger.warn { "No hosts available for scheduling task ${task.name}" }
            return null
        }

        // Group hosts by their cost to handle ties
        val hostsByCost = hosts.groupBy { it.host.getCurrentCost() }

        // Get the group of hosts with the minimum cost
        val minCost = hostsByCost.keys.minOrNull() ?: return null
        val cheapestHosts = hostsByCost[minCost] ?: return null

        // Among hosts with the same (lowest) cost, choose the one with most available memory
        return cheapestHosts.maxByOrNull { it.availableMemory }?.also { selected ->
            logger.info {
                "Selected host ${selected.host.getName()} with cost ${selected.host.getCurrentCost()} " +
                    "for task ${task.name} (available memory: ${selected.availableMemory})"
            }
        }
    }
}
