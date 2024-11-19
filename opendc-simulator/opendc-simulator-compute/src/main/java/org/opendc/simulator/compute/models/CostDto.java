package org.opendc.simulator.compute.models;

public class CostDto {

    private String hostId;
    private Long timestamp;
    private double cost;

    public CostDto(String hostId, Long timestamp, double cost) {
        this.hostId = hostId;
        this.timestamp = timestamp;
        this.cost = cost;
    }

    public String getHostId() {
        return hostId;
    }

    public void setHostId(String hostId) {
        this.hostId = hostId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }
}
