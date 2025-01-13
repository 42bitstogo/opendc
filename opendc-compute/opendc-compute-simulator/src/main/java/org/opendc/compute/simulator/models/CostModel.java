package org.opendc.compute.simulator.models;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
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
    private long startTime;
    private final DateTimeFormatter formatter;

    /**
     * Construct a CostModel
     *
     * @param parentGraph The active FlowGraph which should be used to make the new FlowNode
     * @param costFragments A list of Cost Fragments defining the costs at different time frames
     * @param host The host which should be updated with the cost
     */
    public CostModel(FlowGraph parentGraph, List<CostDto> costFragments, SimHost host, long startTime) {
        super(parentGraph);

        this.host = host;
        this.costFragments = costFragments;
        this.fragmentIndex = 0;
        this.formatter = DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);

        // Initialize with first fragment
        this.currentFragment = this.costFragments.get(this.fragmentIndex);
        this.startTime = startTime;

        // Set initial cost
        this.pushCost(this.currentFragment.getCost());
    }

    /**
     * Convert the given relative time to the absolute time by adding the start time
     */
    private long getAbsoluteTime(long time) {
        return time + startTime;
    }

    /**
     * Convert the given absolute time to the relative time by subtracting the start time
     */
    private long getRelativeTime(long time) {
        return time - startTime;
    }

    /**
     * Find the correct fragment for the given absolute time
     */
    private void findCorrectFragment(long absoluteTime) {
        // Traverse to earlier fragments if needed
        while (absoluteTime < this.currentFragment.getStartTime() && this.fragmentIndex > 0) {
            this.currentFragment = costFragments.get(--this.fragmentIndex);
        }

        // Traverse to later fragments if needed
        while (absoluteTime >= this.currentFragment.getEndTime() && this.fragmentIndex < this.costFragments.size() - 1) {
            this.currentFragment = costFragments.get(++this.fragmentIndex);
        }
    }

    @Override
    public long onUpdate(long now) {
        long absoluteTime = getAbsoluteTime(now);
        System.out.printf("Checking cost at time %d (relative: %d)%n", absoluteTime, now);

        // Check if the current fragment is still valid,
        // Otherwise, find the correct fragment
        if ((absoluteTime < currentFragment.getStartTime()) || (absoluteTime >= currentFragment.getEndTime())) {
            System.out.printf("Current cost fragment invalid: %d - %d%n",
                currentFragment.getStartTime(), currentFragment.getEndTime());

            findCorrectFragment(absoluteTime);

            // If we've exhausted all fragments
            if (fragmentIndex < 0 || fragmentIndex >= costFragments.size() - 1) {
                close();
            }

            pushCost(currentFragment.getCost());
        }

        // Update again at the end of this fragment
        return getRelativeTime(currentFragment.getEndTime());
    }

    private void pushCost(double cost) {
        host.updateCost(cost);
    }

    public void close() {
        this.closeNode();
    }
}
