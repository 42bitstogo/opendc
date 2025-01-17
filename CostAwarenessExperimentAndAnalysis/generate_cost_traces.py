import json
import pandas as pd 
from datetime import datetime, timedelta
import random
import os

def generate_cost_pattern(pattern_type, start_date, end_date, base_cost=500, variation_factor=1.0, num_lines=None):
    """
    Generate cost patterns between given dates
    Added variation_factor to create different but similar patterns for different hosts
    """
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    
    if num_lines:
        total_duration = end - start
        interval = total_duration / (num_lines)
        dates = pd.date_range(start, end, periods=num_lines + 1)
    else:
        dates = pd.date_range(start, end, freq='H')
    
    costs = []
    # Apply variation factor to base cost
    adjusted_base = base_cost * variation_factor
    
    for i in range(len(dates)-1):
        if pattern_type == "stable":
            # Small variations (-10% to +10% of base)
            cost = adjusted_base + random.uniform(-0.1*adjusted_base, 0.1*adjusted_base)
            
        elif pattern_type == "volatile":
            # Large variations (-40% to +40% of base)
            cost = adjusted_base + random.uniform(-0.4*adjusted_base, 0.4*adjusted_base)
            
        elif pattern_type == "diurnal":
            hour = dates[i].hour
            # Higher costs during business hours (8AM-6PM)
            if 8 <= hour <= 18:
                cost = adjusted_base * 1.5 + random.uniform(-0.2*adjusted_base, 0.2*adjusted_base)
            else:
                cost = adjusted_base * 0.7 + random.uniform(-0.1*adjusted_base, 0.1*adjusted_base)
                
        elif pattern_type == "spike":
            # Random price spikes (10% chance of 3x price)
            if random.random() < 0.1:
                cost = adjusted_base * 3 + random.uniform(-0.2*adjusted_base, 0.2*adjusted_base)
            else:
                cost = adjusted_base + random.uniform(-0.1*adjusted_base, 0.1*adjusted_base)
        
        costs.append({
            "startTime": dates[i].strftime("%Y-%m-%d %H:%M:%S"),
            "endTime": dates[i+1].strftime("%Y-%m-%d %H:%M:%S"),
            "cost": max(50, round(cost, 2))  # Ensure minimum cost of 50
        })
    
    return pd.DataFrame(costs)

def generate_all_cost_traces(start_date, end_date, num_hosts=6, num_lines=500):
    """Generate all required cost traces for all patterns and hosts"""
    patterns = ["stable", "volatile", "diurnal", "spike"]
    
    # Create output directories if they don't exist
    for pattern in patterns:
        os.makedirs(f"price/csv/{pattern}", exist_ok=True)
    
    # Generate traces for each pattern and host
    for pattern in patterns:
        print(f"\nGenerating {pattern} cost pattern traces:")
        for host in range(1, num_hosts + 1):
            # Add small random variation for each host (±20% variation)
            variation = random.uniform(0.8, 1.2)
            
            df = generate_cost_pattern(
                pattern, 
                start_date, 
                end_date, 
                base_cost=500,
                variation_factor=variation,
                num_lines=num_lines
            )
            
            output_path = f"price/csv/{pattern}/host{host}_costs.csv"
            df.to_csv(output_path, index=False)
            print(f"  Created {output_path}")

if __name__ == "__main__":
    # Set date range matching workload (as shown in sample data)
    start_date = "2013-08-12"  # Matching workload start
    end_date = "2013-09-30"    # Adjust based on workload duration
    
    # Generate all cost traces
    generate_all_cost_traces(
        start_date=start_date,
        end_date=end_date,
        num_hosts=6,  # For largest topology
        num_lines=500
    )