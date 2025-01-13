package org.opendc.compute.simulator.models;

public class CostDto {
    private Long startTime;
    private Long endTime;
    private double cost;

    public CostDto(Long startTime, Long endTime, double cost) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.cost = cost;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    @Override
    public String toString() {
        return String.format("CostDto{startTime=%d, endTime=%d, cost=%.2f}", startTime, endTime, cost);
    }
}
