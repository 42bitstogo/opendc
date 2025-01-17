# Cost-Aware Resource Management Implementation

This document outlines the implementation of cost awareness in the simulation framework, including cost-based scheduling strategies and analysis tools.

## Overview

The implementation adds cost awareness to the simulation framework, allowing for cost-based decision making in resource allocation. The system reads cost data from parquet files, integrates it into the simulation, and provides different scheduling strategies based on cost considerations.

## Components

### Data Model

#### Cost Data Structure
- `CostDto.java`: Data transfer object representing cost information
  - Contains `startTime`, `endTime`, and `cost` fields
  - Used for transferring cost data between components

#### Cost Data Handling
- `CostModel.java`: Manages cost updates for hosts over time
  - Similar to `CarbonModel`, updates host costs based on time fragments
  - Integrates with `SimHost` to provide dynamic cost information
  - Uses cost traces loaded from parquet files

### File Handling

#### Parquet Integration
- Cost data is stored in parquet files with schema:
  - `startTime` (timestamp)
  - `endTime` (timestamp)
  - `cost` (double)

#### File Processing
- `CostTableReader.kt`: Handles reading cost data from parquet files
- `CostTraceFormat.kt`: Defines the format for cost trace files
- `convert_cost_csv_parquet.py`: Utility for converting CSV cost data to parquet format

### Scheduling Strategies

Three cost-aware scheduling strategies have been implemented:

1. `CostEfficientScheduler.kt`
   - Balances cost with resource efficiency
   - Uses multiple filters including CPU, RAM, and cost ceilings
   - More sophisticated than SimpleCostScheduler but still straightforward

2. `PredictiveCostScheduler.kt`
   - Most sophisticated implementation
   - Uses historical data and exponential smoothing for predictions
   - Considers multiple factors:
     - Predicted future costs
     - Resource utilization
     - Load balancing
   - Configurable weights for different factors

### Integration with Host Management

- `SimHost.kt` modifications:
  - Added cost tracking capabilities
  - Integration with CostModel
  - New methods for cost updates and queries
  - Cost information exported to parquet files

### Analysis Tools

Multiple analysis scripts have been implemented:

1. `analyze_experiments.py`
   - Primary analysis script
   - Compares different scheduling strategies
   - Generates visualizations and metrics
   - Handles cross-pattern analysis

2. `more_analysis.py`
   - Additional detailed analysis
   - Focus on efficiency metrics
   - Task completion analysis
   - Cross-pattern comparisons

### Output and Analysis

The analysis produces several types of outputs:

#### Metrics
- Cost efficiency
- Resource utilization
- Task completion rates
- Performance across different topology sizes

#### Visualizations
- Cost vs. Utilization plots
- Performance by topology
- Cross-pattern comparisons
- Cost efficiency heatmaps

#### Data Files
- CSV exports of analyzed metrics
- Summary reports
- Raw parquet data files with detailed metrics

## Cost Patterns

The system supports different cost patterns for testing:

1. Stable: Small variations around base cost
2. Volatile: Large price fluctuations
3. Diurnal: Day/night price cycles
4. Spike: Random price spikes

## Directory Structure

```
.
├── Utils
│   └── util.py
├── analyze_experiments.py
├── convert_cost_csv_parquet.py
├── experiments
│   ├── cost_experiment_diurnal.json
│   ├── cost_experiment_spike.json
│   ├── cost_experiment_stable.json
│   └── cost_experiment_volatile.json
├── generate_cost_traces.py
├── more_analysis.py
├── output
│   ├── cost_comparison
│   │   ├── diurnal
│   │   │   └── cost_experiment_diurnal
│   │   │       ├── raw-output
│   │   │       │   ├── 0
│   │   │       │   │   └── seed=0
│   │   │       │   │       ├── host.parquet
│   │   │       │   │       ├── powerSource.parquet
│   │   │       │   │       ├── service.parquet
│   │   │       │   │       └── task.parquet
│   │   │       │   ├── 1
│   │   │       ├── simulation-analysis
│   │   │       │   ├── carbon_emission
│   │   │       │   └── power_draw
│   │   │       └── trackr.json
│   │   ├── spike
│   │   │   └── cost_experiment_spike
│   │   ├── stable
│   │   │   └── cost_experiment_stable
│   │   └── volatile
│   │       └── cost_experiment_volatile
│   └── logs
│       ├── analysis_diurnal.log
│       ├── analysis_spike.log
├── price
│   └── csv
│       ├── diurnal
│       │   ├── host1_costs.csv
│       ├── spike
│       │   ├── host1_costs.csv
│       ├── stable
│       │   └── host1_costs.csv
│       └── volatile
│           └── host1_costs.csv
├── price_parquet
│   ├── diurnal
│   │   └── host1_costs.parquet
│   ├── spike
│   │   └── host1_costs.parquet
│   ├── stable
│   │   └── host1_costs.parquet
│   └── volatile
│       └── host1_costs.parquet
├── requirements.txt
├── results
│   ├── {pattern}_cost_vs_util.png
│   ├── {pattern}_performance_metrics.png
│   ├── {pattern}_efficiency_metrics.csv
│   ├── {pattern}_completion_metrics.csv
│   ├── {pattern}_summary.txt
│   ├── cross_pattern_cost.png
│   ├── cross_pattern_completion_rate.png
│   ├── cost_efficiency_heatmap.png
│   ├── cross_pattern_efficiency.csv
│   ├── cross_pattern_completion.csv
│   └── cross_pattern_summary.txt
├── topologies
│   ├── diurnal
│   │   └── topology_small.json
│   ├── spike
│   │   └── topology_small.json
│   ├── stable
│   │   └── topology_small.json
│   └── volatile
│       └── topology_small.json
├── traces
│   └── bitbrains-small
│       ├── fragments.parquet
│       ├── interference-model.json
│       └── tasks.parquet
```

## Setup and Run Instructions

1. **Python Environment Setup**
   ```bash
   # Create and activate virtual environment (recommended)
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install requirements
   pip install -r requirements.txt
   ```

2. **Initial Setup**
   ```bash
   # Create directory structure
   mkdir -p resources/topologies
   mkdir -p price_parquet
   mkdir -p results/
   ```

3. **Generate Cost Data**
   ```bash
   # Generate CSV cost patterns
   python scripts/generate.py
   
   # Convert to parquet format
   python scripts/convert_cost_csv_parquet.py
   ```

4. **Configure Experiment**
   - Update `resources/experiment.json` with desired parameters
   - Ensure topology files are in place
   - Verify cost trace files are properly generated

5. **Run Simulation**
   ```bash
   # First set the working directory to opendc/CostAwarenessExperimentAndAnalysis
   cd /path/to/opendc/CostAwarenessExperimentAndAnalysis
   
   # Add experiment JSON files to the experiments folder
   cp your-experiment.json experiments/
   
   # Run experiment using executable
   $ ./OpenDCExperimentRunner/bin/OpenDCExperimentRunner.sh --experiment-path "experiments/cost_experiment_diurnal.json"
   ```

6. **Run Analysis**
   ```bash
   # Run basic analysis
   python analyze_experiments.py
   
   # Run detailed analysis
   python more_analysis.py
   ```

## Output Structure

```
results/
├── logs/
├── {pattern}_cost_vs_util.png
├── {pattern}_performance_metrics.png
├── {pattern}_efficiency_metrics.csv
├── {pattern}_completion_metrics.csv
├── {pattern}_summary.txt
├── cross_pattern_cost.png
├── cross_pattern_completion_rate.png
├── cost_efficiency_heatmap.png
├── cross_pattern_efficiency.csv
├── cross_pattern_completion.csv
└── cross_pattern_summary.txt
```

## Future Improvements

1. Additional scheduling strategies
2. More sophisticated prediction models
3. Enhanced analysis capabilities
4. Better integration with other metrics (carbon, energy)
5. Real-time cost monitoring and adjustment