package org.opendc.compute.simulator.models;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.opendc.compute.simulator.host.SimHost;
import org.opendc.simulator.engine.FlowGraph;
import org.opendc.simulator.engine.FlowNode;

/**
 * CostModel updates the cost of hosts over time based on provided cost traces.
 */
public class CostModel extends FlowNode {
    private final SimHost host;
    private final List<CostDto> costFragments;
    private CostDto currentFragment;
    private int fragmentIndex;
    private final long startTime;

    public CostModel(FlowGraph parentGraph, List<CostDto> costFragments, SimHost host, long startTime) {
        super(parentGraph);
        this.host = host;
        this.costFragments = costFragments;
        this.fragmentIndex = 0;
        this.startTime = startTime;

        // Initialize with first fragment
        this.currentFragment = this.costFragments.get(this.fragmentIndex);

        // Set initial cost
        pushCost(this.currentFragment.getCost());
    }

    private void findCorrectFragment(long absoluteTime) {
        // Traverse to earlier fragments if needed
        while (absoluteTime < this.currentFragment.getStartTime() && this.fragmentIndex > 0) {
            this.currentFragment = costFragments.get(--this.fragmentIndex);
        }

        // Traverse to later fragments if needed
        while (absoluteTime >= this.currentFragment.getEndTime() &&
                this.fragmentIndex < this.costFragments.size() - 1) {
            this.currentFragment = costFragments.get(++this.fragmentIndex);
        }
        if (this.fragmentIndex < 0 || this.fragmentIndex >= this.costFragments.size() - 1) {
            close();
        }
    }
    public void close() {
        this.closeNode();
    }
    @Override
    public long onUpdate(long now) {
        long absoluteTime = getAbsoluteTime(now);
        System.out.printf("Checking cost at time %d (relative: %d)%n", absoluteTime, now);

        // Check if current fragment is still valid
        if ((absoluteTime < currentFragment.getStartTime()) ||
                (absoluteTime >= currentFragment.getEndTime())) {
            System.out.printf("Current cost fragment invalid: %d - %d%n",
                    currentFragment.getStartTime(), currentFragment.getEndTime());

            findCorrectFragment(absoluteTime);
            pushCost(currentFragment.getCost());
        }

        // Schedule next update at the end of this fragment
        return getRelativeTime(currentFragment.getEndTime());
    }

    private long getAbsoluteTime(long time) {
        return time + startTime;
    }

    private long getRelativeTime(long time) {
        return time - startTime;
    }

    private void pushCost(double cost) {
        host.updateCost(cost);
    }
}
