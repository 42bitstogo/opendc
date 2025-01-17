import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def convert_csv_to_parquet(input_dir, output_dir):
    """Convert CSV files to Parquet maintaining directory structure"""
    os.makedirs(output_dir, exist_ok=True)
    
    # Process each pattern directory
    for pattern in os.listdir(input_dir):
        pattern_input_dir = os.path.join(input_dir, pattern)
        pattern_output_dir = os.path.join(output_dir, pattern)
        
        if not os.path.isdir(pattern_input_dir):
            continue
            
        os.makedirs(pattern_output_dir, exist_ok=True)
        
        # Process each CSV file in the pattern directory
        for filename in os.listdir(pattern_input_dir):
            if not filename.lower().endswith('.csv'):
                continue
                
            input_path = os.path.join(pattern_input_dir, filename)
            output_filename = os.path.splitext(filename)[0] + '.parquet'
            output_path = os.path.join(pattern_output_dir, output_filename)
            
            print(f"Processing '{input_path}' -> '{output_path}'")
            
            # Read CSV
            df = pd.read_csv(input_path, skipinitialspace=True)
            
            # Convert to datetime first
            df['startTime'] = pd.to_datetime(df['startTime'])
            df['endTime'] = pd.to_datetime(df['endTime'])
            
            # Convert datetime to milliseconds epoch
            df['startTime'] = df['startTime'].astype('int64') // 1_000_000  # ns to ms
            df['endTime'] = df['endTime'].astype('int64') // 1_000_000      # ns to ms
            
            # Convert cost to float64
            df['cost'] = df['cost'].astype('float64')
            
            # Define schema
            schema = pa.schema([
                pa.field('startTime', pa.timestamp('ms')),
                pa.field('endTime', pa.timestamp('ms')),
                pa.field('cost', pa.float64())
            ])
            
            # Convert to Arrow Table
            table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            
            # Write to Parquet
            pq.write_table(table, output_path)
            
            print(f"Written schema for '{output_filename}':\n{table.schema}\n")

def convert_cost_traces():
    """Generate CSV files and convert them to Parquet"""
    # Then convert them to Parquet
    input_dir = "price/csv"
    output_dir = "price_parquet"
    
    convert_csv_to_parquet(input_dir, output_dir)
    print(f"All cost traces have been generated and converted to Parquet format in '{output_dir}'")

if __name__ == "__main__":
    convert_cost_traces()