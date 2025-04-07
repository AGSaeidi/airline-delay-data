import os
import pandas as pd
import time
import matplotlib.pyplot as plt
import numpy as np
import gc

def aggregate_data():
    input_file = os.environ.get('INPUT_FILE')
    output_file = os.environ.get('OUTPUT_FILE')
    chunk_size = 10000  # Smaller chunks for 8GB RAM
    
    # Wait for input file to be available
    while not os.path.exists(input_file):
        print(f"Waiting for input file {input_file}...")
        time.sleep(5)
    
    print(f"Starting aggregation from {input_file}")
    
    try:
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Check if required columns exist in the first few rows
        first_chunk = pd.read_csv(input_file, nrows=5)  # Changed from next(pd.read_csv())
        print(f"Available columns: {first_chunk.columns.tolist()}")
        
        # Initialize aggregation variables
        total_rows = 0
        carrier_totals = {}
        
        # Process in chunks
        chunk_reader = pd.read_csv(input_file, chunksize=chunk_size)
        
        for i, chunk in enumerate(chunk_reader):
            print(f"Aggregating chunk {i+1} with {len(chunk)} rows")
            total_rows += len(chunk)
            
            if all(col in chunk.columns for col in ['OP_CARRIER', 'DEP_DELAY', 'ARR_DELAY']):
                # Group by carrier in this chunk
                for carrier, group in chunk.groupby('OP_CARRIER'):
                    if carrier not in carrier_totals:
                        carrier_totals[carrier] = {
                            'dep_delay_sum': 0,
                            'arr_delay_sum': 0,
                            'count': 0
                        }
                    
                    # Update running totals
                    carrier_totals[carrier]['dep_delay_sum'] += group['DEP_DELAY'].sum()
                    carrier_totals[carrier]['arr_delay_sum'] += group['ARR_DELAY'].sum()
                    carrier_totals[carrier]['count'] += len(group)
            else:
                print("Required columns not found, performing basic analysis")
                # Perform basic analysis on numeric columns
                numeric_cols = chunk.select_dtypes(include=[np.number]).columns.tolist()
                if numeric_cols:
                    basic_stats = chunk[numeric_cols].describe()
                    basic_stats.to_csv(output_file)
                    print(f"Basic statistical analysis saved to {output_file}")
                    return
            
            # Free memory
            del chunk
            gc.collect()
        
        # Calculate final averages and create result dataframe
        result_data = []
        for carrier, stats in carrier_totals.items():
            if stats['count'] > 0:
                result_data.append({
                    'OP_CARRIER': carrier,
                    'DEP_DELAY': stats['dep_delay_sum'] / stats['count'],
                    'ARR_DELAY': stats['arr_delay_sum'] / stats['count'],
                    'COUNT': stats['count']
                })
        
        # Create final dataframe and save
        result_df = pd.DataFrame(result_data)
        result_df.to_csv(output_file, index=False)
        print(f"Aggregation complete. Results saved to {output_file}")
        
        # Create chart
        if len(result_df) > 0:
            chart_path = os.path.join(os.path.dirname(output_file), 'delay_chart.png')
            plt.figure(figsize=(12, 8))
            plt.bar(result_df['OP_CARRIER'], result_df['DEP_DELAY'])
            plt.title('Average Departure Delay by Carrier')
            plt.xlabel('Carrier')
            plt.ylabel('Average Delay (minutes)')
            plt.savefig(chart_path)
            print(f"Chart created and saved to {chart_path}")
        
    except Exception as e:
        print(f"Error during aggregation: {str(e)}")
        raise

if __name__ == "__main__":
    aggregate_data()