import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Tuple
import os
from datetime import datetime
import logging
import traceback
import json
from pathlib import Path

def setup_logger(output_dir: str = 'results') -> logging.Logger:
    """Setup logger for analysis with both file and console output."""
    os.makedirs(output_dir, exist_ok=True)
    logger = logging.getLogger('CostAnalysis')
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        # File handler
        fh = logging.FileHandler(os.path.join(output_dir, 'cost_analysis.log'))
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
    
    return logger

def get_scheduler_info(scenario_path: str) -> str:
    """Extract scheduler information from trackr.json."""
    logger = logging.getLogger('CostAnalysis')
    logger.info(f"Getting scheduler info for scenario: {scenario_path}")

    try:
        base_path = os.path.dirname(os.path.dirname(scenario_path))
        trackr_path = os.path.join(base_path, "trackr.json")
        logger.info(f"Looking for trackr.json at: {trackr_path}")

        if os.path.exists(trackr_path):
            with open(trackr_path, 'r') as f:
                configs = json.load(f)
                scenario_num = int(os.path.basename(scenario_path))
                if scenario_num < len(configs):
                    config = configs[scenario_num]
                    scheduler = config.get('allocationPolicy', {}).get('policyType', 'Unknown')
                    logger.info(f"Found scheduler: {scheduler} for scenario {scenario_num}")
                    return scheduler
        
        logger.warning(f"Could not find scheduler info, using filename parsing")
        # Fallback to parsing from path
        if "simplecost" in scenario_path.lower():
            return "SimpleCost"
        elif "costefficient" in scenario_path.lower():
            return "CostEfficient"
        elif "predictivecost" in scenario_path.lower():
            return "PredictiveCost"
        return "Unknown"

    except Exception as e:
        logger.error(f"Error getting scheduler info: {str(e)}")
        logger.error(traceback.format_exc())
        return "Unknown"

def parse_topology_size(scenario_path: str) -> str:
    """Extract topology size by checking both path and trackr.json."""
    logger = logging.getLogger('CostAnalysis')
    
    try:
        # First try from trackr.json
        base_path = os.path.dirname(os.path.dirname(scenario_path))
        trackr_path = os.path.join(base_path, "trackr.json")
        
        if os.path.exists(trackr_path):
            with open(trackr_path, 'r') as f:
                configs = json.load(f)
                scenario_num = int(os.path.basename(scenario_path))
                if scenario_num < len(configs):
                    topology_path = configs[scenario_num].get('topology', {}).get('pathToFile', '')
                    logger.info(f"Found topology path: {topology_path}")
                    
                    # Extract size from topology path
                    if "small" in topology_path.lower():
                        return "small"
                    elif "medium" in topology_path.lower():
                        return "medium"
                    elif "large" in topology_path.lower():
                        return "large"
        
        logger.warning(f"Could not find topology in trackr.json, using path parsing")
        
    except Exception as e:
        logger.error(f"Error parsing topology from trackr.json: {str(e)}")
    
    # Fallback to path parsing
    if "small" in scenario_path.lower():
        return "small"
    elif "medium" in scenario_path.lower():
        return "medium"
    elif "large" in scenario_path.lower():
        return "large"
    return "unknown"

def load_data(base_path: str, cost_pattern: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load and preprocess host and task data."""
    logger = logging.getLogger('CostAnalysis')
    pattern_path = os.path.join(base_path, "cost_comparison", cost_pattern,
                               f"cost_experiment_{cost_pattern}", "raw-output")
    
    logger.info(f"Loading data from path: {pattern_path}")
    if not os.path.exists(pattern_path):
        raise FileNotFoundError(f"Path does not exist: {pattern_path}")
    
    host_dfs = []
    task_dfs = []
    
    for scenario in os.listdir(pattern_path):
        if not scenario.isdigit():
            continue
            
        logger.info(f"Processing scenario: {scenario}")
        scenario_path = os.path.join(pattern_path, scenario)
        logger.info(f"Processing scenario path: {scenario_path}")
        
        for seed_dir in os.listdir(scenario_path):
            if not seed_dir.startswith("seed="):
                continue
                
            logger.info(f"Processing seed directory: {seed_dir}")
            
            host_path = os.path.join(scenario_path, seed_dir, "host.parquet")
            task_path = os.path.join(scenario_path, seed_dir, "task.parquet")
            
            try:
                if os.path.exists(host_path):
                    df = pd.read_parquet(host_path)
                    df['scheduler'] = get_scheduler_info(scenario_path)
                    df['topology'] = parse_topology_size(scenario_path)
                    df['scenario'] = int(scenario)
                    df['seed'] = int(seed_dir.split('=')[1])
                    logger.info(f"Loaded host data: {df.shape}")
                    host_dfs.append(df)
                
                if os.path.exists(task_path):
                    df = pd.read_parquet(task_path)
                    df['scheduler'] = get_scheduler_info(scenario_path)
                    df['topology'] = parse_topology_size(scenario_path)
                    df['scenario'] = int(scenario)
                    df['seed'] = int(seed_dir.split('=')[1])
                    logger.info(f"Loaded task data: {df.shape}")
                    task_dfs.append(df)
                    
            except Exception as e:
                logger.error(f"Error loading data from {scenario_path}: {str(e)}")
                logger.error(traceback.format_exc())
                continue
    
    if not host_dfs or not task_dfs:
        raise ValueError("No data loaded")
        
    return pd.concat(host_dfs, ignore_index=True), pd.concat(task_dfs, ignore_index=True)

def calculate_efficiency_metrics(host_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate efficiency metrics by scheduler and topology."""
    logger = logging.getLogger('CostAnalysis')
    
    try:
        # Filter out SimpleCost data
        host_df = host_df[~host_df['scheduler'].isin(['SimpleCost', 'Unknown'])]
        
        metrics = []
        for (scheduler, topology), group in host_df.groupby(['scheduler', 'topology']):
            metrics.append({
                'scheduler': scheduler,
                'topology': topology,
                'avg_cost': group['cost'].mean(),
                'avg_utilization': group['cpu_utilization'].mean(),
                'cost_efficiency': group['cpu_utilization'].mean() / group['cost'].mean(),
                'std_cost': group['cost'].std(),
                'std_utilization': group['cpu_utilization'].std()
            })
            
        return pd.DataFrame(metrics)
    
    except Exception as e:
        logger.error(f"Error calculating efficiency metrics: {str(e)}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()

def analyze_task_completion(task_df: pd.DataFrame) -> pd.DataFrame:
    """Analyze task completion rates and statistics, excluding SimpleCost."""
    logger = logging.getLogger('CostAnalysis')
    
    try:
        # Filter out SimpleCost data
        task_df = task_df[~task_df['scheduler'].isin(['SimpleCost', 'Unknown'])]
        
        metrics = []
        for (scheduler, topology), group in task_df.groupby(['scheduler', 'topology']):
            completed = group[group['task_state'] == 'COMPLETED']
            completion_times = (completed['finish_time'] - completed['creation_time']).values / 1000  # to seconds
            
            metrics.append({
                'scheduler': scheduler,
                'topology': topology,
                'total_tasks': len(group),
                'completed_tasks': len(completed),
                'completion_rate': len(completed) / len(group) * 100,
                'avg_completion_time': np.mean(completion_times) if len(completion_times) > 0 else np.nan,
                'median_completion_time': np.median(completion_times) if len(completion_times) > 0 else np.nan,
                'std_completion_time': np.std(completion_times) if len(completion_times) > 0 else np.nan
            })
            
        return pd.DataFrame(metrics)
    
    except Exception as e:
        logger.error(f"Error analyzing task completion: {str(e)}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()



def create_visualizations(metrics_df: pd.DataFrame, completion_df: pd.DataFrame, 
                         pattern: str, output_dir: str = 'results'):
    """Create visualization plots for the analysis."""
    logger = logging.getLogger('CostAnalysis')
    
    try:
        # 1. Cost vs Utilization Plot
        plt.figure(figsize=(12, 8))
        for scheduler in metrics_df['scheduler'].unique():
            data = metrics_df[metrics_df['scheduler'] == scheduler]
            plt.scatter(data['avg_cost'], data['avg_utilization'], 
                       label=scheduler, alpha=0.7, s=100)
        
        plt.xlabel('Average Cost')
        plt.ylabel('Average CPU Utilization (%)')
        plt.title(f'Cost vs Utilization Comparison - {pattern.capitalize()} Pattern')
        plt.legend()
        plt.grid(True)
        plt.savefig(os.path.join(output_dir, f'{pattern}_cost_vs_util.png'))
        plt.close()

        # 2. Performance by Topology
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle(f'Performance Metrics by Topology - {pattern.capitalize()} Pattern')
        
        # Cost plot
        sns.barplot(data=metrics_df, x='topology', y='avg_cost', 
                   hue='scheduler', ax=axes[0,0])
        axes[0,0].set_title('Average Cost')
        
        # Utilization plot
        sns.barplot(data=metrics_df, x='topology', y='avg_utilization',
                   hue='scheduler', ax=axes[0,1])
        axes[0,1].set_title('Average CPU Utilization')
        
        # Cost efficiency plot
        sns.barplot(data=metrics_df, x='topology', y='cost_efficiency',
                   hue='scheduler', ax=axes[1,0])
        axes[1,0].set_title('Cost Efficiency (Utilization/Cost)')
        
        # Completion rate plot
        sns.barplot(data=completion_df, x='topology', y='completion_rate',
                   hue='scheduler', ax=axes[1,1])
        axes[1,1].set_title('Task Completion Rate (%)')
        
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f'{pattern}_performance_metrics.png'))
        plt.close()

        logger.info(f"Created visualizations for {pattern} pattern")
        
    except Exception as e:
        logger.error(f"Error creating visualizations: {str(e)}")
        logger.error(traceback.format_exc())

def save_results(metrics_df: pd.DataFrame, completion_df: pd.DataFrame, 
                pattern: str, output_dir: str = 'results'):
    """Save analysis results to CSV files."""
    logger = logging.getLogger('CostAnalysis')
    
    try:
        metrics_df.to_csv(os.path.join(output_dir, f'{pattern}_efficiency_metrics.csv'), index=False)
        completion_df.to_csv(os.path.join(output_dir, f'{pattern}_completion_metrics.csv'), index=False)
        
        # Create summary report
        with open(os.path.join(output_dir, f'{pattern}_summary.txt'), 'w') as f:
            f.write(f"Analysis Summary for {pattern.capitalize()} Pattern\n")
            f.write("=" * 50 + "\n\n")
            
            f.write("Efficiency Metrics Summary:\n")
            f.write(metrics_df.groupby('scheduler').agg({
                'avg_cost': 'mean',
                'avg_utilization': 'mean',
                'cost_efficiency': 'mean'
            }).round(2).to_string())
            f.write("\n\n")
            
            f.write("Task Completion Summary:\n")
            f.write(completion_df.groupby('scheduler').agg({
                'completion_rate': 'mean',
                'avg_completion_time': 'mean'
            }).round(2).to_string())
        
        logger.info(f"Saved analysis results for {pattern} pattern")
        
    except Exception as e:
        logger.error(f"Error saving results: {str(e)}")
        logger.error(traceback.format_exc())

def main():
    # Setup
    output_dir = 'results'
    os.makedirs(output_dir, exist_ok=True)
    logger = setup_logger(output_dir)
    logger.info("Starting cost analysis - Comparing CostEfficient vs PredictiveCost")
    
    base_path = 'output'
    patterns = ['diurnal', 'spike', 'stable', 'volatile']
    
    # Store results for cross-pattern analysis
    all_efficiency_metrics = []
    all_completion_metrics = []
    
    for pattern in patterns:
        logger.info(f"Analyzing {pattern} pattern")
        
        try:
            # Load data
            host_data, task_data = load_data(base_path, pattern)
            
            # Filter out SimpleCost data
            host_data = host_data[~host_data['scheduler'].isin(['SimpleCost', 'Unknown'])]
            task_data = task_data[~task_data['scheduler'].isin(['SimpleCost', 'Unknown'])]
            
            logger.info(f"Loaded data for {pattern} pattern")
            logger.info(f"Host data shape: {host_data.shape}")
            logger.info(f"Task data shape: {task_data.shape}")
            
            if len(host_data) == 0 or len(task_data) == 0:
                logger.warning(f"No valid data found for pattern {pattern} after filtering")
                continue
            
            # Calculate metrics
            efficiency_metrics = calculate_efficiency_metrics(host_data)
            efficiency_metrics['pattern'] = pattern
            all_efficiency_metrics.append(efficiency_metrics)
            
            completion_metrics = analyze_task_completion(task_data)
            completion_metrics['pattern'] = pattern
            all_completion_metrics.append(completion_metrics)
            
            logger.info(f"Calculated metrics for {pattern} pattern")
            
            # Create visualizations
            create_visualizations(efficiency_metrics, completion_metrics, pattern, output_dir)
            
            # Save individual pattern results
            save_results(efficiency_metrics, completion_metrics, pattern, output_dir)
            
        except Exception as e:
            logger.error(f"Error analyzing {pattern} pattern: {str(e)}")
            logger.error(traceback.format_exc())
            continue
    
    # Cross-pattern analysis
    try:
        if all_efficiency_metrics and all_completion_metrics:
            logger.info("Performing cross-pattern analysis")
            
            # Combine metrics across patterns
            all_efficiency_df = pd.concat(all_efficiency_metrics, ignore_index=True)
            all_completion_df = pd.concat(all_completion_metrics, ignore_index=True)
            
            # Create cross-pattern visualizations
            
            # 1. Cost comparison
            plt.figure(figsize=(15, 8))
            sns.barplot(data=all_efficiency_df, 
                       x='pattern', 
                       y='avg_cost', 
                       hue='scheduler')
            plt.title('Average Cost Across Patterns')
            plt.ylabel('Average Cost')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, 'cross_pattern_cost.png'))
            plt.close()
            
            # 2. Cost efficiency heatmap
            plt.figure(figsize=(12, 8))
            pivot_data = all_efficiency_df.pivot_table(
                values='cost_efficiency',
                index='pattern',
                columns='scheduler',
                aggfunc='mean'
            )
            sns.heatmap(pivot_data, annot=True, cmap='YlOrRd', fmt='.2f')
            plt.title('Cost Efficiency Comparison Heatmap')
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, 'cost_efficiency_heatmap.png'))
            plt.close()
            
            # 3. Completion rate comparison
            plt.figure(figsize=(15, 8))
            sns.barplot(data=all_completion_df,
                       x='pattern',
                       y='completion_rate',
                       hue='scheduler')
            plt.title('Task Completion Rate Comparison')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, 'cross_pattern_completion_rate.png'))
            plt.close()
            
            # Save cross-pattern analysis results
            all_efficiency_df.to_csv(os.path.join(output_dir, 'cross_pattern_efficiency.csv'), 
                                   index=False)
            all_completion_df.to_csv(os.path.join(output_dir, 'cross_pattern_completion.csv'),
                                   index=False)
            
            # Generate summary report
            with open(os.path.join(output_dir, 'cross_pattern_summary.txt'), 'w') as f:
                f.write("Cross-Pattern Analysis Summary: CostEfficient vs PredictiveCost\n")
                f.write("=" * 50 + "\n\n")
                
                f.write("Average Cost by Scheduler:\n")
                f.write(all_efficiency_df.groupby('scheduler')['avg_cost']
                       .mean().round(2).to_string())
                f.write("\n\n")
                
                f.write("Average Cost Efficiency by Scheduler:\n")
                f.write(all_efficiency_df.groupby('scheduler')['cost_efficiency']
                       .mean().round(2).to_string())
                f.write("\n\n")
                
                f.write("Average Completion Rate by Scheduler:\n")
                f.write(all_completion_df.groupby('scheduler')['completion_rate']
                       .mean().round(2).to_string())
                f.write("\n\n")
                
                f.write("Best Performing Pattern-Scheduler Combinations:\n")
                best_combos = all_efficiency_df.nlargest(3, 'cost_efficiency')
                f.write(best_combos[['pattern', 'scheduler', 'cost_efficiency']]
                       .to_string(index=False))
                
            logger.info("Completed cross-pattern analysis")
    except Exception as e:
        logger.error(f"Error in cross-pattern analysis: {str(e)}")
        logger.error(traceback.format_exc())
    
    logger.info("Analysis completed")
    logger.info(f"Results saved in {output_dir}")

if __name__ == "__main__":
    main()