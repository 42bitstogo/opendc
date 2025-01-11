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
    private final long startTime = 0L; // Simulation start time
    private final List<CostDto> costTrace;
    private int currentIndex = 0;

    private final DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);
    /**
     * Construct a CostModel.
     *
     * @param parentGraph The FlowGraph to which this node belongs.
     * @param host        The SimHost whose cost is to be updated.
     * @param costTrace   The list of CostDto representing cost over time.
     * @param startTime   The simulation start time.
     */
    public CostModel(FlowGraph parentGraph, List<CostDto> costTrace, SimHost host) {
        super(parentGraph);
        this.host = host;
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
            CostDto currentCostDto = costTrace.get(currentIndex);
            double newCost = currentCostDto.getCost();
            long timestampMillis = currentCostDto.getTimestamp();
            String timestamp = Instant.ofEpochMilli(timestampMillis)
                    .atOffset(ZoneOffset.UTC)
                    .format(formatter);

            System.out.println("CostModel: Updating cost for Host '" + host.getName() + "' (ID: " + host.getUid()
                    + ") at " + timestamp);
            System.out.println("CostModel: New Cost = " + newCost);

            this.host.updateCost(newCost);

            if (currentIndex + 1 < costTrace.size()) {
                long nextTimestamp = costTrace.get(currentIndex + 1).getTimestamp();
                String nextTimeFormatted = Instant.ofEpochMilli(nextTimestamp)
                        .atOffset(ZoneOffset.UTC)
                        .format(formatter);
                System.out.println("CostModel: Scheduling next cost update at " + nextTimeFormatted + "\n");
                return getRelativeTime(nextTimestamp);
            } else {
                System.out.println("CostModel: No more cost updates. Scheduling termination.\n");
                return Long.MAX_VALUE;
            }
        } else {
            System.out.println("CostModel: Cost trace exhausted. No more updates.\n");
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
