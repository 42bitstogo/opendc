package org.opendc.compute.simulator.scheduler

import org.opendc.compute.simulator.scheduler.filters.ComputeFilter
import org.opendc.compute.simulator.scheduler.filters.RamFilter
import org.opendc.compute.simulator.scheduler.filters.VCpuFilter
import org.opendc.compute.simulator.scheduler.filters.CostFilter
import org.opendc.compute.simulator.service.HostView
import org.opendc.compute.simulator.service.ServiceTask

/**
 * A scheduler that balances between cost and resource efficiency using filters.
 */
public class CostEfficientScheduler : ComputeScheduler {
    private val hosts = mutableSetOf<HostView>()

    // Initialize basic filters with standard ratios
    private val filters = mutableListOf(
        ComputeFilter(),
        VCpuFilter(1.0),
        RamFilter(1.5),
        CostFilter(maxCost = 1000.0)  // Set a reasonable cost ceiling
    )

    override fun addHost(host: HostView) {
        hosts.add(host)
    }

    override fun removeHost(host: HostView) {
        hosts.remove(host)
    }

    override fun select(task: ServiceTask): HostView? {
        System.out.println("Selecting host for task ${task.name}")

        // Filter hosts based on all criteria
        val eligibleHosts = hosts.filter { host ->
            filters.all { filter ->
                val passes = filter.test(host, task)
                if (!passes) {
                    System.out.println("Host ${host.host.getName()} failed filter: ${filter.javaClass.simpleName}")
                }
                passes
            }
        }

        if (eligibleHosts.isEmpty()) {
            System.out.println("No eligible hosts found for task ${task.name}")
            return null
        }

        // Select the host with minimum cost among eligible hosts
        return eligibleHosts.minByOrNull { it.host.getCurrentCost() }.also { selectedHost ->
            selectedHost?.let {
                System.out.println("Selected host ${it.host.getName()} with cost ${it.host.getCurrentCost()} for task ${task.name}")
            }
        }
    }
}
