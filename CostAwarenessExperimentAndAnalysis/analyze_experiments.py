import logging
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List

# Import the updated utility functions from your utils.py
# Adjust the import path as needed, for example:
# from Utils.util import (read_parquet_files, read_trackr, get_scenario_label, 
#                         aggregate_runs, get_experiment_mapping)
# For this example, we'll assume it's in the same folder:
from Utils.util import (
    read_parquet_files,
    read_trackr,
    get_scenario_label,
    aggregate_runs
)

VALID_SCHEDULERS = ['CostEfficient', 'PredictiveCost']

def setup_logger(output_dir: str, cost_pattern: str) -> logging.Logger:
    """Setup logger for experiment analysis."""
    log_dir = os.path.join(output_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    logger = logging.getLogger(f'ExperimentAnalyzer_{cost_pattern}')
    logger.setLevel(logging.INFO)
    
    # File handler
    fh = logging.FileHandler(os.path.join(log_dir, f'analysis_{cost_pattern}.log'))
    fh.setLevel(logging.INFO)
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    # Avoid adding multiple handlers if logger is already set
    if not logger.handlers:
        logger.addHandler(fh)
        logger.addHandler(ch)
    
    return logger


class ExperimentAnalyzer:
    def __init__(self, base_path: str, cost_pattern: str):
        self.base_path = base_path
        self.cost_pattern = cost_pattern
        self.logger = setup_logger(base_path, cost_pattern)
        
        self.logger.info(f"Initializing ExperimentAnalyzer for '{cost_pattern}' pattern.")
        
        # Load data
        self.logger.info("Loading parquet files (host, task)...")
        try:
            # Load and filter data
            self.host_df = read_parquet_files(base_path, cost_pattern, "host")
            self.task_df = read_parquet_files(base_path, cost_pattern, "task")
            
            # Filter out SimpleCost data
            self.host_df = self.host_df[self.host_df['scheduler'].isin(VALID_SCHEDULERS)]
            self.task_df = self.task_df[self.task_df['scheduler'].isin(VALID_SCHEDULERS)]
            
            self.logger.info(f"Loaded and filtered host data: {len(self.host_df)} records.")
            self.logger.info(f"Loaded and filtered task data: {len(self.task_df)} records.")
            
            self.trackr = read_trackr(base_path, cost_pattern)
            self.logger.info(f"Loaded trackr data: {len(self.trackr)} scenarios.")
            
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            raise
        
        # Process timestamps
        self.logger.info("Converting timestamps from 'timestamp_absolute' to datetime...")
        for df_name, df in [("host", self.host_df), ("task", self.task_df)]:
            try:
                df['datetime'] = pd.to_datetime(df['timestamp_absolute'], unit='ms')
                self.logger.info(f"Processed timestamps for {df_name} data.")
            except Exception as e:
                self.logger.error(f"Error processing timestamps for {df_name} data: {str(e)}")
                raise
            
        # Generate scenario labels: e.g., "small_cost_diurnal_SimpleCost"
        self.logger.info("Generating scenario labels for each record...")
        for df_name, df in [("host", self.host_df), ("task", self.task_df)]:
            try:
                df['scenario_label'] = (
                    df['topology'] 
                    + '_cost_' + self.cost_pattern 
                    + '_' + df['scheduler']
                )
                unique_scenarios = df['scenario_label'].unique()
                self.logger.info(f"[{df_name}] Unique scenarios: {unique_scenarios}")
            except Exception as e:
                self.logger.error(f"Error generating scenario labels for {df_name} data: {str(e)}")
                raise
    
    def analyze_resource_utilization(self):
        """
        Analyze CPU and memory utilization across different scenarios.
        Aggregates metrics across seeds, and plots CPU utilization over time.
        """
        self.logger.info("Starting resource utilization analysis...")
        
        try:
            # Average across seeds first
            util_metrics = ['cpu_utilization', 'guests_running', 'cpu_time_steal']
            self.logger.info(f"Aggregating utilization metrics: {util_metrics}")
            
            # Group by scenario_label + datetime to get means & std across seeds
            avg_utils = aggregate_runs(
                self.host_df,
                ['scenario_label', 'datetime'],
                util_metrics
            )
            self.logger.info(f"Successfully aggregated metrics across {len(avg_utils)} grouped rows.")
            
            # Flatten MultiIndex columns while preserving 'scenario_label' and 'datetime'
            avg_utils_reset = avg_utils.reset_index()
            self.logger.info(f"Columns before flattening: {avg_utils_reset.columns.tolist()}")
            
            # Define groupby columns that should remain unchanged
            group_cols = ['scenario_label', 'datetime']
            
            # Flatten only the aggregated columns
            new_columns = []
            for col in avg_utils_reset.columns:
                if isinstance(col, tuple):
                    # If the second level is empty, use only the first level
                    if col[1]:
                        new_columns.append(f"{col[0]}_{col[1]}")
                    else:
                        new_columns.append(f"{col[0]}")
                else:
                    new_columns.append(col)
            
            avg_utils_reset.columns = new_columns
            self.logger.info(f"Columns after flattening: {avg_utils_reset.columns.tolist()}")
            
            # Verify that 'scenario_label' exists
            if 'scenario_label' not in avg_utils_reset.columns:
                self.logger.error("'scenario_label' column is missing after flattening.")
                raise KeyError("'scenario_label' column is missing after flattening.")
            
            # Plot CPU utilization over time for each scenario_label
            self.logger.info("Generating CPU utilization plot...")
            plt.figure(figsize=(15, 8))
            
            # Each scenario_label is one line
            for label in avg_utils_reset['scenario_label'].unique():
                scenario_data = avg_utils_reset[avg_utils_reset['scenario_label'] == label]
                
                # Now we can do a simple line plot using scenario_data['datetime'] on the X-axis
                plt.plot(
                    scenario_data['datetime'],
                    scenario_data['cpu_utilization_mean'],
                    label=label
                )
                
            plt.title(f'CPU Utilization Over Time - {self.cost_pattern.capitalize()} Pattern\nCostEfficient vs PredictiveCost')
            plt.xlabel('Time')
            plt.ylabel('CPU Utilization (Mean)')
            plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.grid(True)
            plt.tight_layout()
            
            # Save the figure
            os.makedirs('results', exist_ok=True)
            plot_path = f'results/{self.cost_pattern}_cpu_utilization.png'
            plt.savefig(plot_path)
            plt.close()
            self.logger.info(f"Saved CPU utilization plot to {plot_path}")
            
            return avg_utils_reset
        
        except KeyError as ke:
            self.logger.error(f"KeyError in resource utilization analysis: {str(ke)}")
            raise
        except Exception as e:
            self.logger.error(f"Error in resource utilization analysis: {str(e)}")
            raise

    def analyze_cost_metrics(self):
        """
        Analyze cost-related metrics. Aggregates cost & CPU utilization, computes cost efficiency, 
        and plots a comparison bar chart.
        """
        self.logger.info("Starting cost metrics analysis...")
        
        try:
            cost_metrics = ['cost', 'cpu_utilization']
            self.logger.info(f"Analyzing cost metrics: {cost_metrics}")
            
            # Calculate average cost metrics across seeds
            avg_costs = aggregate_runs(
                self.host_df,
                ['scenario_label', 'topology', 'scheduler'],
                cost_metrics
            )
            self.logger.info(f"Aggregated cost metrics: {len(avg_costs)} scenario group(s).")
            
            # Flatten MultiIndex columns while preserving groupby columns
            avg_costs_reset = avg_costs.reset_index()
            self.logger.info(f"Columns before flattening: {avg_costs_reset.columns.tolist()}")
            
            # Define groupby columns that should remain unchanged
            group_cols = ['scenario_label', 'topology', 'scheduler']
            
            # Flatten only the aggregated columns
            new_columns = []
            for col in avg_costs_reset.columns:
                if isinstance(col, tuple):
                    # If the second level is empty, use only the first level
                    if col[1]:
                        new_columns.append(f"{col[0]}_{col[1]}")
                    else:
                        new_columns.append(f"{col[0]}")
                else:
                    new_columns.append(col)
            
            avg_costs_reset.columns = new_columns
            self.logger.info(f"Columns after flattening: {avg_costs_reset.columns.tolist()}")
            
            # Verify that necessary columns exist
            required_cols = ['cpu_utilization_mean', 'cpu_utilization_std', 
                             'cost_mean', 'cost_std']
            for rc in required_cols:
                if rc not in avg_costs_reset.columns:
                    self.logger.error(f"Required column '{rc}' is missing after flattening.")
                    raise KeyError(f"Required column '{rc}' is missing after flattening.")
            
            # Calculate cost efficiency
            self.logger.info("Calculating cost efficiency metrics...")
            avg_costs_reset['cost_efficiency'] = (
                avg_costs_reset['cpu_utilization_mean'] / 
                avg_costs_reset['cost_mean']
            )

            # Generate plot: bar chart of cost & cost efficiency
            self.logger.info("Generating cost comparison plot...")
            plt.figure(figsize=(15, 8))
            
            scenarios = avg_costs_reset['scenario_label']
            x = np.arange(len(scenarios))
            width = 0.35
            
            plt.bar(x - width/2, avg_costs_reset['cost_mean'], width,
                    label='Average Cost',
                    yerr=avg_costs_reset['cost_std'])
            plt.bar(x + width/2, avg_costs_reset['cost_efficiency'], width,
                    label='Cost Efficiency')
            
            plt.title(f'Cost Metrics Comparison - {self.cost_pattern.capitalize()} Pattern\nCostEfficient vs PredictiveCost')
            plt.xlabel('Scenario')
            plt.ylabel('Metrics')
            plt.xticks(x, scenarios, rotation=45, ha='right')
            plt.legend()
            plt.tight_layout()
            
            plot_path = f'results/{self.cost_pattern}_cost_comparison.png'
            plt.savefig(plot_path)
            plt.close()
            self.logger.info(f"Saved cost comparison plot to {plot_path}")
            
            self.logger.info("Cost Summary Statistics:")
            for _, row in avg_costs_reset.iterrows():
                self.logger.info(f"\nScenario: {row['scenario_label']}")
                self.logger.info(f"  Topology: {row['topology']}")
                self.logger.info(f"  Scheduler: {row['scheduler']}")
                self.logger.info(f"  Avg Cost: {row['cost_mean']:.2f}")
                self.logger.info(f"  Cost Std Dev: {row['cost_std']:.2f}")
                
                # Safely access cost_efficiency
                if pd.notnull(row['cost_efficiency']):
                    ce_value = float(row['cost_efficiency'])
                    self.logger.info(f"  Cost Efficiency: {ce_value:.4f}")
                else:
                    ce_value = None
                    self.logger.info("  Cost Efficiency: None")
            
            return avg_costs_reset  # Ensure to return the DataFrame
                
        except KeyError as ke:
            self.logger.error(f"KeyError in cost metrics analysis: {str(ke)}")
            raise
        except Exception as e:
            self.logger.error(f"Error in cost metrics analysis: {str(e)}")
            raise

    def analyze_task_completion(self):
        """
        Analyze task completion statistics: completion rates, average times, etc.
        Generate relevant plots (completion rates, state distribution, boxplots).
        Returns a DataFrame of scenario-level stats.
        """
        self.logger.info("Starting task completion analysis...")
        
        try:
            scenario_stats = {}
            unique_scenarios = self.task_df['scenario_label'].unique()
            
            for scenario in unique_scenarios:
                self.logger.info(f"Analyzing scenario: {scenario}")
                
                # Filter tasks for this scenario
                scenario_tasks = self.task_df[self.task_df['scenario_label'] == scenario]
                
                # Group by task_id to find final (last) state
                task_final_states = scenario_tasks.groupby('task_id').last()
                
                # State distribution
                state_counts = task_final_states['task_state'].value_counts()
                total_tasks = len(task_final_states)
                
                # Completion times (only for COMPLETED tasks)
                completed_tasks = task_final_states[task_final_states['task_state'] == 'COMPLETED']
                if len(completed_tasks) > 0:
                    completion_times = (completed_tasks['finish_time'] - completed_tasks['creation_time']) / 1000.0
                else:
                    completion_times = pd.Series(dtype=float)
                
                # Stats
                metrics = {
                    'total_tasks': total_tasks,
                    'completion_rate': state_counts.get('COMPLETED', 0) / total_tasks if total_tasks > 0 else 0,
                    'termination_rate': state_counts.get('TERMINATED', 0) / total_tasks if total_tasks > 0 else 0,
                    'error_rate': state_counts.get('ERROR', 0) / total_tasks if total_tasks > 0 else 0,
                    'avg_completion_time': completion_times.mean() if not completion_times.empty else None,
                    'median_completion_time': completion_times.median() if not completion_times.empty else None,
                    'std_completion_time': completion_times.std() if not completion_times.empty else None,
                    'min_completion_time': completion_times.min() if not completion_times.empty else None,
                    'max_completion_time': completion_times.max() if not completion_times.empty else None
                }
                
                scenario_stats[scenario] = metrics
                
                # Log partial results
                self.logger.info(f"  Total Tasks: {metrics['total_tasks']}")
                self.logger.info(f"  Completion Rate: {metrics['completion_rate']:.2%}")
                if metrics['avg_completion_time'] is not None:
                    self.logger.info(f"  Average Completion Time: {metrics['avg_completion_time']:.2f}s")
                else:
                    self.logger.info("  No completed tasks.")
            
            stats_df = pd.DataFrame.from_dict(scenario_stats, orient='index')
            
            # --- Visualizations ---
            self.logger.info("Generating task completion visualizations...")
            os.makedirs('results', exist_ok=True)
            
            # 1) Completion Rates
            plt.figure(figsize=(12, 6))
            stats_df['completion_rate'].plot(kind='bar')
            plt.title(f'Task Completion Rates - {self.cost_pattern.capitalize()} Pattern\nCostEfficient vs PredictiveCost')
            plt.xlabel('Scenario')
            plt.ylabel('Completion Rate')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            plot_path = f'results/{self.cost_pattern}_completion_rates.png'
            plt.savefig(plot_path)
            plt.close()
            self.logger.info(f"Saved completion rates plot to {plot_path}")
            
            # 2) Completion Times Box Plot
            plt.figure(figsize=(12, 6))
            scenario_completion_times = []
            scenario_labels = []
            
            # Collect completion times per scenario
            for scenario in unique_scenarios:
                scenario_tasks = self.task_df[
                    (self.task_df['scenario_label'] == scenario)
                    & (self.task_df['task_state'] == 'COMPLETED')
                ]
                if not scenario_tasks.empty:
                    c_times = (scenario_tasks['finish_time'] - scenario_tasks['creation_time']) / 1000
                    scenario_completion_times.append(c_times)
                    scenario_labels.append(scenario)
            
            if scenario_completion_times:
                plt.boxplot(scenario_completion_times, labels=scenario_labels)
                plt.title(f'Task Completion Times Distribution - {self.cost_pattern.capitalize()} Cost Pattern')
                plt.xlabel('Scenario')
                plt.ylabel('Completion Time (seconds)')
                plt.xticks(rotation=45, ha='right')
                plt.tight_layout()
                plot_path = f'results/{self.cost_pattern}_completion_times.png'
                plt.savefig(plot_path)
                plt.close()
                self.logger.info(f"Saved completion times plot to {plot_path}")
            
            # 3) State Distribution (stacked bar)
            state_distributions = []
            for scenario in unique_scenarios:
                scenario_tasks = self.task_df[self.task_df['scenario_label'] == scenario]
                final_states = scenario_tasks.groupby('task_id')['task_state'].last()
                state_dist = final_states.value_counts(normalize=True)
                state_distributions.append(state_dist)
            
            state_dist_df = pd.DataFrame(state_distributions, index=unique_scenarios).fillna(0.0)
            
            plt.figure(figsize=(12, 6))
            state_dist_df.plot(kind='bar', stacked=True)
            plt.title(f'Task State Distribution - {self.cost_pattern.capitalize()} Cost Pattern')
            plt.xlabel('Scenario')
            plt.ylabel('Proportion of Tasks')
            plt.legend(title='Task State', bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.tight_layout()
            plot_path = f'results/{self.cost_pattern}_state_distribution.png'
            plt.savefig(plot_path)
            plt.close()
            self.logger.info(f"Saved state distribution plot to {plot_path}")
            
            return stats_df
        except Exception as e:
            self.logger.error(f"Error in task completion analysis: {str(e)}")
            raise

    def comparative_analysis(self, cost_metrics_df: pd.DataFrame, completion_stats_df: pd.DataFrame):
        """
        Compare different scheduling algorithms across topologies for the same cost pattern.
        Combines provided cost metrics and completion metrics into a single DataFrame 
        and produces a multi-metric bar chart.
        """
        self.logger.info("Starting comparative analysis...")
        
        try:
            # Convert both into an easy-to-merge structure
            comparison_rows = []
            for _, row in cost_metrics_df.iterrows():
                scen_label = row['scenario_label']
                topology = row['topology']
                scheduler = row['scheduler']
                # We'll fetch the completion_rate from the completion_stats_df if it exists
                if scen_label in completion_stats_df.index:
                    comp_rate = completion_stats_df.loc[scen_label, 'completion_rate']
                else:
                    comp_rate = None  # scenario not found
                
                comparison_rows.append({
                    'scenario_label': scen_label,
                    'topology': topology,
                    'scheduler': scheduler,
                    'avg_cost': row['cost_mean'],
                    'cost_std': row['cost_std'],
                    'cpu_utilization': row['cpu_utilization_mean'],
                    'cost_efficiency': row['cost_efficiency'],
                    'completion_rate': comp_rate
                })
            
            comparison_df = pd.DataFrame(comparison_rows)
            
            self.logger.info("Generating comparative bar plots...")
            if not comparison_df.empty:
                fig, axes = plt.subplots(2, 2, figsize=(15, 12))
                fig.suptitle(f'CostEfficient vs PredictiveCost Comparison\n{self.cost_pattern.capitalize()} Pattern')
                
                metrics = ['avg_cost', 'cpu_utilization', 'cost_efficiency', 'completion_rate']
                titles = ['Average Cost', 'CPU Utilization', 'Cost Efficiency', 'Task Completion Rate']
                
                for i, (metric, title) in enumerate(zip(metrics, titles)):
                    ax = axes[i//2, i%2]
                    if metric in comparison_df.columns:
                        sns.barplot(
                            data=comparison_df,
                            x='topology', y=metric,
                            hue='scheduler', ax=ax
                        )
                        ax.set_title(title)
                        ax.tick_params(axis='x', rotation=45)
                    else:
                        self.logger.warning(f"Metric '{metric}' not found in comparison data.")
                
                plt.tight_layout(rect=[0, 0.03, 1, 0.95])  # Adjust layout to accommodate the suptitle
                # plt.subplots_adjust(hspace=0.4)
                plot_path = f'results/{self.cost_pattern}_comparative_analysis.png'
                plt.savefig(plot_path)
                plt.close()
                self.logger.info(f"Saved comparative analysis plot to {plot_path}")
            else:
                self.logger.warning("No data available for comparison plots.")
            
            return comparison_df
                
        except Exception as e:
            self.logger.error(f"Error in comparative analysis: {str(e)}")
            raise


def main():
    base_path = 'output'
    
    try:
        # Create results directory
        os.makedirs('results', exist_ok=True)
        logger = logging.getLogger('ExperimentAnalysis')
        logger.setLevel(logging.INFO)
        
        # Add handlers to the main logger if not already present
        if not logger.handlers:
            # File handler
            fh = logging.FileHandler(os.path.join('results', 'main_analysis.log'))
            fh.setLevel(logging.INFO)
            
            # Console handler
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            
            # Create formatter
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)
            
            logger.addHandler(fh)
            logger.addHandler(ch)
        
        logger.info("Starting experiment analysis (main)...")
        
        # Analyze each cost pattern
        for pattern in ['stable', 'volatile', 'diurnal', 'spike']:
            logger.info(f"\nAnalyzing '{pattern}' cost pattern...")
            
            analyzer = ExperimentAnalyzer(base_path, pattern)
            
            # Perform all analyses
            # 1) Resource stats (CPU utilization, etc.)
            resource_stats = analyzer.analyze_resource_utilization()
            # 2) Cost stats
            cost_stats = analyzer.analyze_cost_metrics()
            # 3) Task stats
            task_stats = analyzer.analyze_task_completion()
            # 4) Comparison across different schedulers
            comparison = analyzer.comparative_analysis(cost_metrics_df=cost_stats, completion_stats_df=task_stats)
            
            # Save CSV outputs
            logger.info(f"Saving results for '{pattern}' pattern...")
            resource_stats.to_csv(f'results/{pattern}_resource_stats.csv', index=False)
            cost_stats.to_csv(f'results/{pattern}_cost_stats.csv', index=False)
            task_stats.to_csv(f'results/{pattern}_task_stats.csv')
            comparison.to_csv(f'results/{pattern}_comparison.csv', index=False)
            
        logger.info("Analysis completed successfully.")
        
    except Exception as e:
        logger.error(f"Error in main analysis: {str(e)}")
        raise


if __name__ == "__main__":
    main()