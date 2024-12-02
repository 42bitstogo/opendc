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

package org.opendc.simulator.compute.models;

import java.util.List;
import org.opendc.compute.simulator.host.SimHost;
import org.opendc.simulator.engine.FlowGraph;
import org.opendc.simulator.engine.FlowNode;

/**
 * CostModel updates the cost of hosts over time based on provided cost traces.
 */
public class CostModel extends FlowNode {

    private final SimHost host;
    private final long startTime; // Simulation start time
    private final List<CostDto> costTrace;
    private int currentIndex = 0;

    /**
     * Construct a CostModel.
     *
     * @param parentGraph The FlowGraph to which this node belongs.
     * @param host        The SimHost whose cost is to be updated.
     * @param costTrace   The list of CostDto representing cost over time.
     * @param startTime   The simulation start time.
     */
    public CostModel(FlowGraph parentGraph, List<CostDto> costTrace, SimHost host, long startTime) {
        super(parentGraph);
        this.host = host;
        this.startTime = startTime;
        this.costTrace = costTrace;

        if (!costTrace.isEmpty()) {
            // Initialize host cost
            CostDto initialCostDto = costTrace.get(currentIndex);
            host.updateCost(initialCostDto.getCost());
        }
    }

    @Override
    public long onUpdate(long now) {
        currentIndex++;
        if (currentIndex < costTrace.size()) {
            // Update host cost
            CostDto currentCostDto = costTrace.get(currentIndex);
            host.updateCost(currentCostDto.getCost());

            // Schedule next update
            if (currentIndex + 1 < costTrace.size()) {
                long nextTimestamp = costTrace.get(currentIndex + 1).getTimestamp();
                return getRelativeTime(nextTimestamp);
            } else {
                // No more updates
                return Long.MAX_VALUE;
            }
        } else {
            // No more cost data
            return Long.MAX_VALUE;
        }
    }
    /**
     * Convert absolute time to relative time.
     */
    private long getRelativeTime(long absoluteTime) {
        return absoluteTime - startTime;
    }

    public void close() {
        this.closeNode();
    }
}
