import pandas as pd
import os
import re
import json
from typing import Dict, List, Tuple

def read_trackr(base_path: str, cost_pattern: str) -> List[Dict]:
    """
    Read the trackr.json file which maps scenario indices to topology, workloads, etc.
    """
    trackr_path = os.path.join(base_path, "cost_comparison", cost_pattern,
                               f"cost_experiment_{cost_pattern}", "trackr.json")
    with open(trackr_path) as f:
        return json.load(f)

def read_parquet_files(base_path: str, cost_pattern: str, file_type: str) -> pd.DataFrame:
    """
    Read and combine all parquet files of a given 'file_type' (e.g. host, task, service...)
    from each scenario and seed. The scenario index in 'trackr.json' is assumed to match
    the folder structure in 'raw-output'.
    """
    dfs = []
    pattern_path = os.path.join(base_path, "cost_comparison", cost_pattern,
                                f"cost_experiment_{cost_pattern}", "raw-output")
    
    # Read trackr for scenario mapping
    trackr_data = read_trackr(base_path, cost_pattern)
    
    for scenario_num in os.listdir(pattern_path):
        if not scenario_num.isdigit():
            continue
            
        scenario_path = os.path.join(pattern_path, scenario_num)
        # This picks the scenario info from trackr.json by index
        scenario_info = trackr_data[int(scenario_num)]
        
        # Extract topology size from pathToFile (small, medium, large)
        match = re.search(r'topology_(small|medium|large)',
                          scenario_info['topology']['pathToFile'])
        topology = match.group(1) if match else 'unknown'
        
        scheduler = scenario_info['allocationPolicy']['policyType']
        
        # Read each seed directory
        for seed_dir in os.listdir(scenario_path):
            if not seed_dir.startswith("seed="):
                continue
                
            file_path = os.path.join(scenario_path, seed_dir, f"{file_type}.parquet")
            if not os.path.exists(file_path):
                continue
                
            df = pd.read_parquet(file_path)
            df['topology'] = topology
            df['scheduler'] = scheduler
            df['scenario'] = int(scenario_num)
            df['seed'] = int(seed_dir.split('=')[1])
            
            dfs.append(df)
    
    if not dfs:
        raise ValueError(f"No {file_type}.parquet files found for {cost_pattern} pattern.")
        
    return pd.concat(dfs, ignore_index=True)


def get_experiment_mapping(base_path: str, cost_pattern: str) -> pd.DataFrame:
    """
    Create a mapping DataFrame showing (scenario -> topology, scheduler).
    Extracts 'small', 'medium', or 'large' from each scenario's topology path.
    """
    trackr_data = read_trackr(base_path, cost_pattern)
    
    mapping = []
    for i, scenario in enumerate(trackr_data):
        # Extract 'small', 'medium', 'large' from the path
        match = re.search(r'topology_(small|medium|large)',
                          scenario['topology']['pathToFile'])
        topology = match.group(1) if match else 'unknown'
        
        mapping.append({
            'scenario': i,
            'topology': topology,
            'scheduler': scenario['allocationPolicy']['policyType']
        })
    
    return pd.DataFrame(mapping)


def get_scenario_label(topology: str, cost_pattern: str, scheduler: str) -> str:
    """Generate consistent scenario label, e.g. 'small_cost_diurnal_SimpleCost'."""
    return f"{topology}_cost_{cost_pattern}_{scheduler}"


def aggregate_runs(df: pd.DataFrame, group_cols: List[str], metric_cols: List[str]) -> pd.DataFrame:
    """
    Group by specified columns (plus topology & scheduler if they exist in df but
    aren't already in group_cols) and compute mean + std of the given metric columns.
    """
    agg_dict = {col: ['mean', 'std'] for col in metric_cols}
    
    # Ensure topology and scheduler are included in grouping if they exist
    if 'topology' not in group_cols and 'topology' in df.columns:
        group_cols.append('topology')
    if 'scheduler' not in group_cols and 'scheduler' in df.columns:
        group_cols.append('scheduler')
    
    grouped = df.groupby(group_cols).agg(agg_dict).round(4)
    return grouped

def test_utils():
    """
    Quick tests for utility functions. 
    Adjust 'cost_pattern' for an actual test scenario.
    """
    base_path = "output"
    cost_pattern = "diurnal"
    
    try:
        # 1) Test reading trackr.json
        trackr_data = read_trackr(base_path, cost_pattern)
        print(f"Successfully read trackr.json with {len(trackr_data)} scenarios.")
        
        # 2) Test reading parquet files
        host_df = read_parquet_files(base_path, cost_pattern, "host")
        print("\nHost DataFrame Info:")
        print(host_df.info())
        
        # 3) Print experiment mapping
        mapping_df = get_experiment_mapping(base_path, cost_pattern)
        print("\nExperiment Mapping:")
        print(mapping_df)
        
    except Exception as e:
        print(f"Error in test_utils(): {e}")


if __name__ == "__main__":
    # Run quick tests when this file is executed directly
    test_utils()